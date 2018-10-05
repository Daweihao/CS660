### Lab 1  Writeup

#### 1.Design decisions

In the Tuple.java and TupleDesc.java, I have implemented several attributes and methods that is related to each attribute. Especially for fieldtype and fieldname. For all of the tupledesc, I use arraylist to represent the overall information for them.

In the Catalog.java, there is not too much to design. I create a class to store the information with a particular table. And initializing them with the proper attribute.

In the BufferPool.java, I created a concurrentHashMap to record the bufferspace. And the getPage can retrieve the page both from the bufferPool and from the database files.

In the HeapPageId.java and RecordId.java, all the attributes deal with hashing the Id to be the unique number in the database.

In the HeapPage.java, the most tricky part is to read the exact slot in a bit wise way.

#### 2.Changes to API

#### 3.Missing or Incomplete element

#### 4.The time I have spent and the toughest part

Around 18 hours.