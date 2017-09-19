/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package btree;

import java.io.*;

import org.w3c.dom.Attr;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;
/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	/**
	 * It causes a structured trace to be written to a file. This output is used
	 * to drive a visualization tool that shows the inner workings of the b-tree
	 * during its operations.
	 *
	 * @param filename
	 *            input parameter. The trace file name
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	/**
	 * Stop tracing. And close trace file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this
	 *         btree file.
	 */
	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno)
			throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty)
			throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 *
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not ger the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException,
			PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BTreeFile's header page; -
		 * headerPage, headerPageId valid and pinned - dbname contains a copy of
		 * the name of the database
		 */
	}

	/**
	 * if index file exists, open it; else create it.
	 *
	 * @param filename
	 *            file name. Input parameter.
	 * @param keytype
	 *            the type of key. Input parameter.
	 * @param keysize
	 *            the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            full delete or naive delete. Input parameter. It is either
	 *            DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException
	 *                can not get file
	 * @exception ConstructPageException
	 *                page constructor failed
	 * @exception IOException
	 *                error from lower layer
	 * @exception AddFileEntryException
	 *                can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize,
			int delete_fashion) throws GetFileEntryException,
			ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 *
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 * @exception IteratorException
	 *                iterator error
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception FreePageException
	 *                error when free a page
	 * @exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException,
			UnpinPageException, FreePageException, DeleteFileEntryException,
			ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException,
			IteratorException, PinPageException, ConstructPageException,
			UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page,
					headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage
					.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException,
			PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */);

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/**
	 * insert record with the given key and rid
	 *
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iterator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */
	public void insert(KeyClass key, RID rid) throws KeyTooLongException,
			KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException,
			UnpinPageException, PinPageException, NodeNotMatchException,
			ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException,
			IOException

	{
		
		PageId Temp_RootPageNo = headerPage.get_rootId();

		PageId temp_inv_page = new PageId(INVALID_PAGE);

		if (Temp_RootPageNo.pid == INVALID_PAGE) /*
													 * If no Entry Exists in the
													 * tree i.e. the tree is
													 * empty
													 */
		{

			BTLeafPage FirstRootPage = new BTLeafPage(
					AttrType.attrInteger); /* Create a new Leaf Page */

			pinPage(FirstRootPage.getCurPage()); /* Pin the page */

			FirstRootPage.insertRecord(key, rid); /* insert the key */

			unpinPage(FirstRootPage.getCurPage(), true); /* Unpin the page */

			updateHeader(FirstRootPage
					.getCurPage()); /*
									 * Update the header , i.e make the leaf
									 * page = root of the tree
									 */

			FirstRootPage.setNextPage(
					temp_inv_page); /*
									 * Set the next and previous page to Invalid
									 * Pages as single page exists
									 */
			FirstRootPage.setPrevPage(temp_inv_page);

			if (trace != null) {
				trace.writeBytes("VISIT node " + FirstRootPage.getCurPage() + lineSep);
				trace.flush();
			}

		}

		else { /*
				 * Else if tree is not empty, Call _insert method to insert the
				 * entry at its correct place
				 */

			try {
				KeyDataEntry UpEntry = _insert(Integer.parseInt(key.toString()), rid, Temp_RootPageNo);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InsertRecException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IndexFullDeleteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
	}

	private KeyDataEntry _insert(int key, RID rid, PageId currentPageId)  // Assuming the key is an Integer value
			throws PinPageException, IOException, ConstructPageException,
			LeafDeleteException, ConstructPageException, DeleteRecException,
			IndexSearchException, UnpinPageException, LeafInsertRecException,
			ConvertException, IteratorException, IndexInsertRecException,
			KeyNotMatchException, NodeNotMatchException, InsertException, InsertRecException, IndexFullDeleteException

	{
		Page temp_page = new Page();

		PageId temp_inv_page = new PageId(INVALID_PAGE);

		temp_page = pinPage(currentPageId);
		
		final RID data_rid=rid;//new RID();
		
		//data_rid=rid;

		BTSortedPage temp_sorted_page = new BTSortedPage(temp_page,
				AttrType.attrInteger); /*
										 * Create an instance of BTSorted Page to
										 * check the type of page
										 */

		KeyDataEntry UpEntry = null; /* UpEntry that a page gets */

		KeyDataEntry idx_UpEntry = null; /* UpEntry that a page returns */

		if (temp_sorted_page
				.getType() == NodeType.INDEX) { /*
												 * If the node type is Index -
												 * Call _insert again on next
												 * routed page
												 */
			BTIndexPage currentindexpage = new BTIndexPage(temp_page, AttrType.attrInteger);

			PageId curr_index_page_id = currentindexpage
					.getCurPage(); /* Get the Current Index Entry Page No */

			PageId temp_next_page = currentindexpage.getPageNoByKey(new IntegerKey(
					key)); /* Scroll to the Correct child using the Key */

			BTSortedPage temp_sort_next_page = new BTSortedPage(temp_next_page,
					AttrType.attrInteger); /*
											 * Initialize an instance of next
											 * page
											 */

			UpEntry = _insert(key, rid,
					temp_next_page); /* Call _insert on the Child Page */

			PageId temp_next_page_2 = currentindexpage.getPageNoByKey(new IntegerKey(key));

			if (UpEntry != null) { /*
									 * If Up Entry is not null - Implies Split is
									 * there
									 */
				if (currentindexpage.available_space() > BT.getKeyDataLength(new IntegerKey(key),
						NodeType.INDEX)) { /*
											 * If Space is there in the existing
											 * page - Insert the value and
											 * return null
											 */
					pinPage(currentindexpage.getCurPage());

					currentindexpage.insertKey(UpEntry.key, temp_sort_next_page
							.getNextPage()); /*
												 * Insert the value in the index
												 * page and return null
												 */

					unpinPage(currentindexpage.getCurPage(),
							true); /* unpin the page */

					if (trace != null) {
						trace.writeBytes("VISIT node " + currentindexpage.getCurPage() + lineSep);
						trace.flush();
					}
				}

				else /*
						 * Else if Space is not there in the Index Page - Index
						 * Page needs to be Split
						 */
				{
					BTIndexPage Split_indexpage = new BTIndexPage(
							AttrType.attrInteger); /* Create a new index page */

					Page temp_page_idx = pinPage(Split_indexpage.getCurPage());

					RID idx_del_rid = new RID(); /*
													 * Create an instance of RID
													 * class
													 */

					KeyDataEntry idx_temp_key = currentindexpage.getFirst(idx_del_rid);

					int idx_count = 1;

					int idx_split_point = 1;

					RID idx_del_rid_2 = new RID();

					idx_count = (currentindexpage.getSlotCnt() + 1) / 2;

					KeyDataEntry idx_split_temp_key = currentindexpage.getFirst(
							idx_del_rid); /*
											 * creating a key data entry to
											 * navigate the index tree
											 */

					while (idx_split_point < currentindexpage
							.getSlotCnt()) { /*
												 * Insertion into the new index
												 * page after the split
												 */
						if (idx_split_point > idx_count + 1) {
							Split_indexpage.insertKey(idx_split_temp_key.key,
									currentindexpage.getPageNoByKey(idx_split_temp_key.key));
							idx_split_temp_key = currentindexpage.getNext(idx_del_rid);
						} else
							idx_split_temp_key = currentindexpage.getNext(idx_del_rid);

						if (idx_split_point == idx_count) {
							idx_UpEntry = idx_split_temp_key;

							idx_del_rid_2 = idx_del_rid;
						}

						idx_split_point++;
					}

					Split_indexpage.insertKey(UpEntry.key, temp_sort_next_page
							.getNextPage()); /*
												 * Inserting the Push Up/Copy Up
												 * Entry
												 */
					Split_indexpage.setLeftLink(currentindexpage.getPageNoByKey(idx_UpEntry.key));

					unpinPage(Split_indexpage.getCurPage(), true);

					if (trace != null) {
						trace.writeBytes("VISIT node " + Split_indexpage.getCurPage() + lineSep);
						trace.flush();
					}

					int del_flag = 1;

					int total_idx_slots = currentindexpage.getSlotCnt();

					KeyDataEntry idx_split_temp_key_2 = currentindexpage.getFirst(idx_del_rid_2);

					if (currentindexpage.getCurPage().pid == headerPage
							.get_rootId().pid) { /*
													 * If Split happens at root -
													 * Creating a new index page
													 * and assigning it as root
													 */
						BTIndexPage temp_index_page = new BTIndexPage(AttrType.attrInteger);

						pinPage(temp_index_page.getCurPage());

						temp_index_page.insertKey(idx_UpEntry.key, Split_indexpage.getCurPage());

						unpinPage(temp_index_page.getCurPage(), true);

						updateHeader(temp_index_page.getCurPage()); // Catch-
																	// Correct

						temp_index_page.setLeftLink(currentindexpage.getCurPage());

						if (trace != null) {
							trace.writeBytes("VISIT node " + temp_index_page.getCurPage() + lineSep);
							trace.flush();
						}
					}

					KeyDataEntry idx_split_temp_key_3;

					KeyClass del_key = null;

					int i = 0;

					KeyDataEntry temp_arr[] = new KeyDataEntry[total_idx_slots];

					while (del_flag < total_idx_slots) { /*
															 * Storing the values
															 * to be deleted
															 * from existing
															 * split index page
															 * after the split
															 */
						if (BT.keyCompare(idx_UpEntry.key, idx_split_temp_key_2.key) == 0)
							del_key = idx_split_temp_key_2.key;

						if (BT.keyCompare(idx_UpEntry.key, idx_split_temp_key_2.key) <= 0) {

							temp_arr[i] = currentindexpage.getNext(idx_del_rid_2);

							i++;

						} else {
							idx_split_temp_key_2 = currentindexpage.getNext(idx_del_rid_2);
						}

						del_flag++;

					}

					for (int j = 0; j < i; j++) { /*
													 * Deleting the values from
													 * existing split index page
													 * after the split
													 */
						currentindexpage.deleteKey(temp_arr[j].key);
					}

					currentindexpage.deleteKey(idx_UpEntry.key);

					unpinPage(currentindexpage.getCurPage(), true);
				}
			}

			return idx_UpEntry;
		}

		if (temp_sorted_page.getType() == NodeType.LEAF) { /*
															 * If the node type
															 * is of type Leaf
															 */
			BTLeafPage currentleafpage = new BTLeafPage(temp_page, AttrType.attrInteger);

			PageId curr_leaf_page_id = currentleafpage.getCurPage();

			/*
			 * If the space is available in the leaf page - directly insert the
			 * value
			 */
			if (currentleafpage.available_space() > BT.getKeyDataLength(new IntegerKey(key), NodeType.LEAF))

			{
				pinPage(currentleafpage.getCurPage());

				currentleafpage.insertRecord(new IntegerKey(key), rid);

				unpinPage(currentleafpage.getCurPage(), true);

			} else { /*
						 * Else if space is full - Split will happen at the leaf
						 * page
						 */

				// System.out.println("No Space left for record to insert :
				// Split will occur");
				
							
				PageId curr_key_page=data_rid.pageNo;
				
				BTLeafPage split_new_leafpage = new BTLeafPage(
						AttrType.attrInteger); /*
												 * Create a new BTLeaf Page where
												 * new entries will be copied
												 */

				pinPage(split_new_leafpage.getCurPage());

				/*
				 * Set the pointers correctly to next and prev pages so that all
				 * leaf pages behave as a doubly linked list
				 */

				split_new_leafpage.setPrevPage(curr_leaf_page_id);

				split_new_leafpage.setNextPage(temp_inv_page);

				currentleafpage.setNextPage(split_new_leafpage.getCurPage());

				KeyDataEntry temp_key = currentleafpage.getFirst(rid);

				RID del_rid = new RID();

				RID del_rid_2 = new RID();

				int count = 1, flag = 0, del_flag = 1;

				del_rid_2 = rid;

				/* Calculating where the new key should go */
				
				while (count < currentleafpage.getSlotCnt() / 2) {
					temp_key = currentleafpage.getNext(rid);

					if (count == currentleafpage.getSlotCnt() / 2 - 2) {

						flag = BT.keyCompare(new IntegerKey(key), temp_key.key);
						if (flag < 0) {
							del_rid = rid;
							break;
						}

					}

					count++;

					del_rid = rid;

				}
				
				
				KeyDataEntry split_temp_key = currentleafpage.getCurrent(rid);

				int split_point = 1;

				while (count < currentleafpage
						.getSlotCnt()) { /*
											 * Inserting Split records into new
											 * split leaf page
											 */

					split_temp_key = currentleafpage.getNext(del_rid);

					split_new_leafpage.insertRecord(split_temp_key);

					if (split_point == 1) { /* Finding the copy up value */
						UpEntry = split_temp_key;
					}

					count++;

					split_point++;
				}
				
				
				KeyDataEntry split_temp_key_2 = currentleafpage.getFirst(del_rid_2);

				int total_slots = currentleafpage.getSlotCnt();

				while (del_flag <= total_slots) { /*
													 * Deleting the values from
													 * existing leaf page that
													 * are copied to new page
													 * after the split
													 */
					if (BT.keyCompare(UpEntry.key, split_temp_key_2.key) <= 0) {
						currentleafpage.delEntry(split_temp_key_2);
						split_temp_key_2 = currentleafpage.getCurrent(del_rid_2);
					}

					else
						split_temp_key_2 = currentleafpage.getNext(del_rid_2);

					del_flag++;
				}
				
				
				if (flag > 0) /* Deciding on which page the new key will go */
					split_new_leafpage.insertRecord(new IntegerKey(key), new RID(curr_key_page,key));

				else
					currentleafpage.insertRecord(new IntegerKey(key), new RID(curr_key_page,key));

				if (currentleafpage
						.getPrevPage().pid == -1) { /*
													 * If split happens to be at
													 * the root page - create a
													 * new index page and set it
													 * as root of the tree
													 */

					BTIndexPage temp_index_page = new BTIndexPage(AttrType.attrInteger);

					pinPage(temp_index_page.getCurPage());

					temp_index_page.insertKey(UpEntry.key, split_new_leafpage.getCurPage());

					unpinPage(temp_index_page.getCurPage());

					updateHeader(temp_index_page.getCurPage());

					temp_index_page.setLeftLink(currentleafpage.getCurPage());

					if (trace != null) {
						trace.writeBytes("VISIT node " + temp_index_page.getCurPage() + lineSep);
						trace.flush();
					}
				}

				unpinPage(split_new_leafpage.getCurPage(),true);

				if (trace != null) {
					trace.writeBytes("VISIT node " + split_new_leafpage.getCurPage() + lineSep);
					trace.flush();
				}

			}

		}

		/* return the correct entry as per the page */
		if (temp_sorted_page.getType() == NodeType.INDEX)
			return idx_UpEntry;

		else
			return UpEntry;

	}
	



	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry;
	 * it is not the id of the data entry)
	 *
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 *
	 */
	public boolean Delete(KeyClass key, RID rid) throws DeleteFashionException,
			LeafRedistributeException, RedistributeException,
			InsertRecException, KeyNotMatchException, UnpinPageException,
			IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException,
			IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException,
			IOException {
		if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
			return NaiveDelete(key, rid);
		else
			throw new DeleteFashionException(null, "");
	}

	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 * 
	 * find left-most occurrence of `lo_key', going all the way left if lo_key
	 * is null.
	 * 
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 * 
	 * Since we allow duplicates, this must "go left" as described in the text
	 * (for the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way
	 * left if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
	 */

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException,
			IteratorException, KeyNotMatchException, ConstructPageException,
			PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null
					&& BT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (BT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno),
						headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}

	/*
	 * Status BTreeFile::NaiveDelete (const void *key, const RID rid)
	 * 
	 * Remove specified data entry (<key, rid>) from an index.
	 * 
	 * We don't do merging or redistribution, but do allow duplicates.
	 * 
	 * Page containing first occurrence of key `key' is found for us by
	 * findRunStart. We then iterate for (just a few) pages, if necesary, to
	 * find the one containing <key,rid>, which we then delete via
	 * BTLeafPage::delUserRid.
	 */

	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException,
			ConstructPageException, IOException, UnpinPageException,
			PinPageException, IndexSearchException, IteratorException {
	// remove the return statement and start your code.
		try {
			BTLeafPage TempPage = new BTLeafPage(AttrType.attrInteger);
			TempPage = findRunStart(key,
					rid); /* Find the leaf Page where the record is present */

			if (TempPage == null) /*
									 * Implies the entered key value does not
									 * exist
									 */
			{
				System.out.println("Key not found !!");
				return false;
			} else {
				if (BT.keyCompare(key, TempPage.getCurrent(
						rid).key) == 0) /*
										 * Condition due to a logical error in
										 */
				{
					pinPage(TempPage.getCurPage());
					TempPage.deleteSortedRecord(
							rid); /*
									 * Delete the record from the page
									 * findRunStart method
									 */
					unpinPage(TempPage.getCurPage(), true);

					if (trace != null) {
						trace.writeBytes("VISIT node " + TempPage + lineSep);
						trace.flush();
					}

					return true;
				}

				else {
					System.out.println(
							"Key not found !!"); /*
													 * Trying to delete the already
													 * deleted key
													 */
					return false;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}
	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null
	 * scan the whole index (2) lo_key = null, hi_key!= null range scan from min
	 * to the hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key
	 * to max (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (
	 * might not unique) (5) lo_key!= null, hi_key!= null, lo_key < hi_key range
	 * scan from lo_key to hi_key
	 *
	 * @param lo_key
	 *            the key where we begin scanning. Input parameter.
	 * @param hi_key
	 *            the key where we stop scanning. Input parameter.
	 * @exception IOException
	 *                error from the lower layer
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception UnpinPageException
	 *                error when unpin a page
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws IOException, KeyNotMatchException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id) throws IOException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

}
