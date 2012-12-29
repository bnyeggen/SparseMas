package nyeggen.sparsemas;

import java.util.Arrays;

import gnu.trove.TIntArrayList;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TLongObjectHashMap;

//With a separate struct for an entry, can accommodate ~220M entries in ~12gb
//of heap, or ~55 bytes / entry (includes per-person & per-item overhead)
//With parallel arrays (basically saving the 8 byte object overhead) that
//should become ~10.3gb at the expense of some implementation complexity

//The idea here is to support "expansion" with no copying or recalculating of
//indexes, and iteration of filled elements by row and column.  Despite the 
//name, as of right now does not support many "traditional" matrix methods, 
//although they are mostly easy to implement on top of it.  A wrapper class
//would be the way to go if you really need linear algebra support.
public class SparseMas {
  //Row,Column->Index mapping moves outward in spiral
	//personID and itemID are integers >=0.
	//personID is y axis, itemID is x axis.
	//17 19 21 23 24
	//10 12 14 15 22
	//5  7  8  13 20
	//2  3  6  11 18
	//0  1  4  9  16
	//This can be nigh any long->object mapping (eg MapDB, or Mahout 
	//FastByIDMap) so long as it's efficient.
	private final TLongObjectHashMap<MatrixNode> storage;
	//Each person to their "lowest" itemID, and vice versa.  Really it's just
	//the head of the linked list, so not guaranteed to be the minimum unless
	//it's sorted.
	//These could be stored as sorted packed longs and binary-searched at lower
	//storage overhead.
	//Since we're constrained to >=0 IDs, we could use the negative bit as a
	//flag to represent sortedness (cleared on insert, until a subsequent sort
	//call is made)
	private final TIntIntHashMap minPersonForItem;
	private final TIntIntHashMap minItemForPerson;
	
	//Returns the element to look up in the hashmap, based on person and item
	//These are all unique and avoid overflow so long as person & item are ints
	//>= 0
	//Supporting transposition would be as easy as having a "transposed"
	//boolean that switches between this and an implementation where person
	//and int are flipped.
	public static long calculateIndex(int person, int item){
		//Actual element is <= this
		final long diagonal;
		final int difference = Math.abs(person-item);
		if (person>item){
			diagonal = ((long)person+1)*((long)person+1);
		} else {
			diagonal = ((long)item+1)*((long)item+1) - 1;
		} 
		return diagonal - difference * 2;
	}
	
	//Ideally we'd have this as 3 parallel arrays with the indexes calculated
	//via a hash algo to avoid overhead, but this is a much simpler
	//implementation for now
	public static class MatrixNode {
		//These can be flags, integers in the byte range, whatever.
		//Higher-precision datatypes are overkill for my use case, but you
		//may wish to switch it to use them, or for that matter, to attach
		//additional metadata (dates, etc.)
		//A new payload constitutes a new node.
		private final byte payload;
		//nextPerson and nextItem are 0 when there is no (known) next value
		//(eg, end-of-list)
		//Another option is, instead of pointers to next person / item, have
		//pointers to the next filled element.  Increased traversal time, 
		//decreased space.  Could potentially cache the by-person & by-item 
		//mapping separately from the single traversal.
		private int nextPerson;
		private int nextItem;
		public MatrixNode(byte payload) {
			this(payload,0,0);
		}
		public MatrixNode(byte payload, int nextPerson, int nextItem){
			this.payload = payload;
			this.nextItem = nextItem;
			this.nextPerson = nextPerson;
		}
		public int getNextPerson() {
			return nextPerson;
		}
		public void setNextPerson(int person) {
			nextPerson = person;
		}
		public int getNextItem(){
			return nextItem;
		}
		public void setNextItem(int item) {
			nextItem = item;
		}
		public byte getPayload(){
			return payload;
		}
	}

	public SparseMas() {
		minPersonForItem = new TIntIntHashMap();
		minItemForPerson = new TIntIntHashMap();
		storage = new TLongObjectHashMap<MatrixNode>();
	}
	
	//For pre-allocating underlying storage to avoid expanding to more than
	//we need.
	public SparseMas(int personStorage, int itemStorage, int keyStorage){
		minPersonForItem = new TIntIntHashMap(itemStorage);
		minItemForPerson = new TIntIntHashMap(personStorage);
		storage = new TLongObjectHashMap<MatrixNode>(keyStorage);
	}
	
	//Whether we have a value for the given person/item
	public boolean contains(int person, int item) {
		return storage.containsKey(calculateIndex(person, item));
	}
	
	//Reduce the underlying storage to accommodate the current number of items
	//plus ~11%.
	public void compact(){
		storage.compact();
	}
	
	//Current number of stored entries.
	public int size(){
		return storage.size();
	}
	
	//All people currently stored.  Not returned in a particular order.
	public int[] people(){
		return minItemForPerson.keys();
	}
	
	//All items currently stored.  Not returned in a particular order.
	public int[] items(){
		return minPersonForItem.keys();
	}
	
	//The item at the head of the linked list traversing all items for the
	//given person, or 0 if there is none.
	public int minItemForPerson(int person){
		return minItemForPerson.get(person);
	}
	//The person at the head of the linked list traversing all the people for
	//the given item, or 0 if there is none.
	public int minPersonForItem(int item){
		return minPersonForItem.get(item);
	}
	
	//The next person after the given person/item combo in traversal order, or
	//0 if there is none.
	public int nextPerson(int person, int item){
		if (!contains(person,item)) return 0;
		return storage.get(calculateIndex(person, item)).getNextPerson();
	}
	//The next item after the given person/item combo in traversal order, or 0
	//if there is none.
	public int nextItem(int person, int item){
		if (!contains(person,item)) return 0;
		return storage.get(calculateIndex(person, item)).getNextItem();
	}
	//Return the payload for the given person/item combo, or 0 if there is none
	//(if 0 is a valid value, should precede by checking contains(person,item))
	public byte get(int person, int item){
		if(!contains(person,item)) return 0;
		return storage.get(calculateIndex(person, item)).getPayload();
	}
	//Return the raw MatrixNode stored for the given person/item combo, or null
	//if there is none.
	public MatrixNode getNode(int person, int item) {
		return storage.get(calculateIndex(person, item));
	}
	
	//Insert a new MatrixNode with the given payload for the given person/item
	//combo.  By default it is inserted at the head of the traversal linked
	//list, not maintaining sorted order.
	public void insert(int person, int item, byte payload){
		final long index = calculateIndex(person, item);
		storage.put(index, new MatrixNode(payload));
		final MatrixNode insertion = storage.get(index);
		final int prevMinPerson = minPersonForItem(item);
		final int prevMinItem = minItemForPerson(person);
		
		if(prevMinItem!=0) insertion.setNextItem(prevMinItem);
		if(prevMinPerson!=0) insertion.setNextPerson(prevMinPerson);
		
		minPersonForItem.put(item, person);
		minItemForPerson.put(person, item);
	}
	
	//Insert a new MatrixNode with the given payload for the given person/item
	//combo.  Maintain sorted order by doing an insertion sort in the traversal
	//list.  May actually be faster to do a normal insert + sort.
	public void insertSorted(int person, int item, byte payload){
		throw new UnsupportedOperationException("insertSorted not implemented yet");
	}
	
	//Array of all items associated with the given person. Not guaranteed to
	//be in a particular order.
	public int[] itemsForPerson(int person){
		TIntArrayList out = new TIntArrayList();
		int thisItem = minItemForPerson(person);
		while(thisItem!=0){
			out.add(thisItem);
			thisItem=nextItem(person, thisItem);
		}
		return out.toNativeArray();
	}
	
	//Array of all people associated with the given item.  Not guaranteed to be
	//in a particular order.
	public int[] peopleForItem(int item) {
		TIntArrayList out = new TIntArrayList();
		int thisPerson = minPersonForItem(item);
		while(thisPerson!=0){
			out.add(thisPerson);
			thisPerson=nextPerson(thisPerson, item);
		}
		return out.toNativeArray();
	}
	
	//Mutably sort the item pointers for the traversal list for the given
	//person, so they traverse in ascending order.
	//It's threadsafe to do all of these in parallel
	public void sortItemPointersForPerson(int person){
		int[] items = itemsForPerson(person);
		if (items.length==1 || items.length==0) return;
		Arrays.sort(items);
		minItemForPerson.put(person, items[0]);
		int i;
		for(i=0;i+1<items.length;i++){
			getNode(person, items[i])
			.setNextItem(items[i+1]);
		}
		getNode(person, items[i]).setNextItem(0);
	}
	
	//Mutably sort the person pointers for the traversal list for the given
	//item, so they traverse in ascending order.
	//It's threadsafe to do all of these in parallel	
	public void sortPersonPointersForItem(int item){
		int[] people = peopleForItem(item);
		if (people.length==1 || people.length==0) return;
		Arrays.sort(people);
		minItemForPerson.put(people[0], item);
		int i;
		for(i=0;i+1<people.length;i++){
			final MatrixNode node = getNode(people[i], item);
			if(node==null){
				throw new RuntimeException("Null node: "+people[i]+", "+item);
			} else node.setNextPerson(people[i+1]);
		}
		getNode(people[i], item).setNextPerson(0);
	}
	
	//Mutably sort all item and person pointers, so any traversal happens in
	//ascending order.  Operates concurrently with the given number of threads.
	public void sortAllPointers(final int nThreads){
		Thread[] threads = new Thread[nThreads];
		for(int i=0;i<nThreads;i++){
			final int j = i;
			Thread thisThread = new Thread(new Runnable() {
				@Override
				public void run() {
					for(final int item:items()){
						if(item%nThreads==j) sortPersonPointersForItem(item);
					}
					for(final int person:people()){
						if(person%nThreads==j) sortItemPointersForPerson(person);
					}
				}
			});
			thisThread.start();
			threads[i] = thisThread;
		}
		for(Thread t:threads){
			try{
				t.join();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public void sortAllPointers(){
		sortAllPointers(1);
	}
	
	//Find the person that points (along with the given item) via nextPerson to
	//the given combo.  Relies on traversal, and so operates in avg O(n) time.
	//Returns 0 if there is no such pointer.
	public int prevPerson(int person, int item){
		if(!contains(person, item)) return 0;
		int prevPerson = minPersonForItem(item);
		if (prevPerson==person) return 0;
		int thisPerson = nextPerson(prevPerson, item);
		while(thisPerson<person && thisPerson!=0){
			prevPerson = thisPerson;
			thisPerson = nextPerson(prevPerson, item);
		}
		return prevPerson;
	}
	
	//Find the item that points (along with the given person) via nextItem to
	//the given combo.  Relies on traversal, and so operates in avg O(n) time.
	//Returns 0 if there is no such pointer.
	public int prevItem(int person, int item){
		if(!contains(person, item)) return 0;
		int prevItem = minItemForPerson(person);
		if (prevItem==item) return 0;
		int thisItem = nextItem(person, prevItem);
		while(thisItem<item && thisItem!=0){
			prevItem = thisItem;
			thisItem = nextItem(person, prevItem);
		}
		return prevItem;
	}
	
	//Remove the given person,item combo from the matrix, adjusting the
	//pointers when necessary.  Runs in avg O(n) time since it relies on
	//traversal to find previous nodes.
	public void remove(int person, int item){
		//Point prior person node at next person (or 0 if there is no next)
		//If there is no prior, then set min person to next (or just remove if
		//there is no next)
		{
			final int prevPerson = prevPerson(person, item);
			final int nextPerson = nextPerson(person, item);
			if(prevPerson==0 && nextPerson==0)
				minPersonForItem.remove(item);
			else if(prevPerson==0)
				minPersonForItem.put(item, nextPerson);
			else if (nextPerson==0)
				getNode(prevPerson, item).setNextPerson(0);
			else
				getNode(prevPerson, item).setNextPerson(nextPerson);
		}
		//And equivalent for item
		{
			final int prevItem = prevItem(person, item);
			final int nextItem = nextItem(person, item);
			if(prevItem==0 && nextItem==0)
				minItemForPerson.remove(person);
			else if (prevItem==0)
				minItemForPerson.put(person, nextItem);
			else if (nextItem==0)
				getNode(person, prevItem).setNextItem(0);
			else
				getNode(person,prevItem).setNextItem(nextItem);
		}
		storage.remove(calculateIndex(person, item));
	}
	
	//Remove all nodes for the given item.  More efficient than repeatedly
	//calling remove (O(n) vs O(n^2)).
	public void removeItem(int item){
		int[] people = peopleForItem(item);
		for(final int person:people) storage.remove(calculateIndex(person, item));
		minPersonForItem.remove(item);
	}
	
	//Remove all nodes for the given person.  More efficient than repeatedly
	//calling remove (O(n) vs O(n^2)).
	public void removePerson(int person){
		int[] items = itemsForPerson(person);
		for(final int item:items) storage.remove(calculateIndex(person, item));
		minItemForPerson.remove(person);
	}
}
