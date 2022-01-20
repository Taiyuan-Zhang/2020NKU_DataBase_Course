# 2020NKU_DataBase_Course

repo for 2020NKU_DataBase_Course, final project is to write a basic database management system called SimpleDB.

这个项目记录了2020年南开大学计算机学院《数据库系统》（计算机科学卓越班）的课程大作业，基于伯克利大学数据库基础的大作业，实现一个基础数据库管理系统：SimpleDB。

### Lab1: SimpleDB

这里是[Lab1文档](lab1.md)

#### Exercise 1.

**Implement the skeleton methods in:**

---

- src/simpledb/TupleDesc.java
- src/simpledb/Tuple.java

---

#### Exercise 2.

**Implement the skeleton methods in:**

***

 * src/simpledb/Catalog.java

***

#### Exercise 3.

**Implement the `getPage()` method in:**

***

* src/simpledb/BufferPool.java

***

#### Exercise 4.

**Implement the skeleton methods in:**

***

*    src/simpledb/HeapPageId.java
*    src/simpledb/RecordID.java
*    src/simpledb/HeapPage.java 

***

#### Exercise 5.

**Implement the skeleton methods in:**

***

* src/simpledb/HeapFile.java

***

#### Exercise 6.

**Implement the skeleton methods in:**

***

* src/simpledb/SeqScan.java

***

### Lab2: SimpleDB Operators

这里是[Lab2文档](lab2.pdf)

#### Exercise 1.

**Implement the skeleton methods in:**

---

- src/simpledb/Predicate.java
- src/simpledb/JoinPredicate.java
- src/simpledb/Filter.java
- src/simpledb/Join.java

---

#### Exercise 2.

**Implement the skeleton methods in:**

---

- src/simpledb/IntegerAggregator.java
- src/simpledb/StringAggregator.java
- src/simpledb/Aggregate.java

---

#### Exercise 3.

**Implement the skeleton methods in:**

---

- src/simpledb/HeapPage.java
- src/simpledb/HeapPage.java

---

#### Exercise 4.

**Implement the skeleton methods in:**

---

- src/simpledb/Insert.java
- src/simpledb/Delete.java

---

#### Exercise 5.

**Fill in the flushPage() method and additional helper methods to implement page eviction in**

---

- src/simpledb/BufffferPool.java

---

### Lab3: B+ Tree Index

这里是[Lab3文档](lab3.pdf)

#### Exercise 1:  BTreeFile.fifindLeafPage()

**Implement BTreeFile.findLeafPage() .**

----

#### Exercise 2:  Splitting Pages

**Implement BTreeFile.splitLeafPage() and BTreeFile.splitInternalPage() .**

----

#### Exercise 3: Redistributing and merging pages

**Implement BTreeFile.stealFromLeafPage() , BTreeFile.stealFromLeftInternalPage() ,BTreeFile.stealFromRightInternalPage() , BTreeFile.mergeLeafPages() and BTreeFile.mergeInternalPages().**

----

#### Exercise 4: (10% extra credit)

**Implement the skeleton methods in:**

---

#### Exercise 5

**Create and implement a class called BTreeReverseScan which scans the BTreeFile in reverse,given an optional IndexPredicate .** 

----

### Lab4: SimpleDB Transactions

这里是[Lab4文档](lab4.pdf)

#### Exercise 1.

**Write the methods that acquire and release locks in BufffferPool. Assuming you are using page-level locking, you will need to complete the following:**

---

- Modify getPage() to block and acquire the desired lock before returning a page.
- Implement releasePage() .This method is primarily used for testing, and at the end of transactions.
- Implement holdsLock() so that logic in Exercise 2 can determine whether a page is already locked by a transaction.

---

#### Exercise 2.

**Ensure that you acquire and release locks throughout SimpleDB. Some (but not necessarily all) actions that you should verify work properly:**

---

#### Exercise 3.

**Implement the necessary logic for page eviction without evicting dirty pages in the evictPage method in BufferPool .**

---

#### Exercise 4.

**Implement the transactionComplete() method in BufferPool . **

---

#### Exercise 5.

**Implement deadlock detection and resolution in src/simpledb/BufferPool.java .** 

---

#### Bonus Exercise 6.（10% extra credit）

**For one or more of these choices, implement both alternatives and brieflfly compare their performance characteristics in your writeup.**

