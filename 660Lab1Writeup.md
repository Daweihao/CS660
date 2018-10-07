### Lab 1  Writeup

#### 1.Design decisions

In the Tuple.java and TupleDesc.java, I have implemented several attributes and methods that is related to each attribute. Especially for fieldtype and fieldname. For all of the tupledesc, I use arraylist to represent the overall information for them.

In the Catalog.java, there is not too much to design. I create a class to store the information with a particular table. And initializing them with the proper attribute.

In the BufferPool.java, I created a concurrentHashMap to record the bufferspace. And the getPage can retrieve the page both from the bufferPool and from the database files.

In the HeapPageId.java and RecordId.java, all the attributes deal with hashing the Id to be the unique number in the database.

In the HeapPage.java, the most tricky part is to read the exact slot in a bit wise way.First, I use the BitInterger to do the bitwise operation. Then I find it little confusing and I use shift operation instead. And the one mistake I made here is to fetch the headersize. I divided it using 8, which is wrong. I found it using 2 days debugging.

In the HeapFile.java, I use the randomaccessFile class to do the file scanning.

In the SeqScan.java, it combines the java files I have implemented to go through an entire process. The method of getTupleDesc() is a little tricky, I have thought about it for a while. It is just like reorganizing the tupleDesc. Apart from that, I have created  a HeapFileIterator whose job is to fetch the correct page on the file and know the right end of it.

#### 2.Changes to API

New classes: HeapPageIterator, HeapFileIterator.

HeapPageIterator's job is to scan each tuple in that heap page.

HeapFileIterator has to deal with open a particular page including calculating the correct page id of current page, check whether it is the last and rewind operation.

#### 3.Missing or Incomplete element

None.

#### 4.The time I have spent and the toughest part

Around 18 hours. Passing the seqscantest takes me a lot of time. Especially for that parameter setting ( 8 or 8.0 choice selection).