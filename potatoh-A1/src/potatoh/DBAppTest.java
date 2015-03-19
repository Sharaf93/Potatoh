package potatoh;

import java.io.*;
import java.util.*;

public class DBAppTest {
	public DBAppTest() {

	}

	protected void testTableCreate() throws Exception {
		DBApp app;
		Hashtable htblCols;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		System.out.println("testcase: testTableCreate ");

		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert1() throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		htblVals = new Hashtable();
		htblVals.put("ID", Integer.toString(100));
		htblVals.put("Name", "John Smith");
		htblVals.put("Department", "Sales");
		htblVals.put("Salary", Float.toString(12000.05f));
		htblVals.put("Insured", "True");

		app.insertIntoTable("Employee", htblVals);

		System.out.println("testcase: testTableCreateInsert1");
		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert100() throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 100; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			htblVals.put("Department", "Sales");
			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));
			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}

		System.out.println("testcase: testTableCreateInsert100");
		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert450() throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 450; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			htblVals.put("Department", "Sales");
			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));
			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}
		System.out.println("testcase: testTableCreateInsert450");

		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert250Delete125() throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals, htblConditions;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 250; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			htblVals.put("Department", "Sales");
			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));
			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}

		htblConditions = new Hashtable();
		htblConditions.put("Insured", "True");

		app.deleteFromTable("Employee", htblConditions, "");
		System.out.println("testcase: testTableCreateInsert250Delete125");

		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert250SelectDelete125Select()
			throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals, htblConditions;
		int nCounter1 = 0, nCounter2 = 0;
		Iterator iterator;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 250; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			htblVals.put("Department", "Sales");
			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));
			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}

		htblConditions = new Hashtable();
		htblConditions.put("Insured", "True");

		iterator = app.selectRangeFromTable("Employee", htblConditions, "");
		while (iterator.hasNext()) {
			Object obj;
			obj = iterator.next();
			nCounter1++;
		}

		app.deleteFromTable("Employee", htblConditions, "");

		iterator = app.selectRangeFromTable("Employee", htblConditions, "");
		while (iterator.hasNext()) {
			Object obj;
			obj = iterator.next();
			nCounter2++;
		}
		System.out
				.println("testcase: testTableCreateInsert250SelectDelete125Select");
		System.out
				.println("             running first select returned (correct answer = 125): "
						+ nCounter1);
		System.out
				.println("             running second select returned (correct answer = 0): "
						+ nCounter2);

		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert75BuildIndex() throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Age", "Java.lang.Integer");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 75; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			htblVals.put("Department", "Sales");
			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));
			htblVals.put("Insured", "True");
			htblVals.put("Age", Integer.toString(nIndex + 18));
			app.insertIntoTable("Employee", htblVals);
		}

		app.createIndex("Employee", "Age");
		System.out.println("testcase: testTableCreateInsert75BuildIndex");

		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert2000SelectBuildIndexSelect()
			throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals, htblConditions;
		int nCounter1 = 0, nCounter2 = 0, nAgeGroup;
		Iterator iterator;
		long lTimeStart, lTimeEnd, lTimeWithoutBPlus, lTimeWithBPlus;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("AgeGroup", "Java.lang.Integer");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 2000; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			htblVals.put("Department", "Sales");
			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));
			nAgeGroup = 1 + (int) (Math.random() * ((10 - 1) + 1));

			htblVals.put("AgeGroup", Integer.toString(nAgeGroup));

			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}

		htblConditions = new Hashtable();
		htblConditions.put("AgeGroup", "5");

		lTimeStart = System.currentTimeMillis();

		iterator = app.selectRangeFromTable("Employee", htblConditions, "");
		while (iterator.hasNext()) {
			Object obj;
			obj = iterator.next();
			nCounter1++;
		}

		lTimeEnd = System.currentTimeMillis();
		lTimeWithoutBPlus = lTimeEnd - lTimeStart;

		app.createIndex("Employee", "AgeGroup");

		lTimeStart = System.currentTimeMillis();
		iterator = app.selectRangeFromTable("Employee", htblConditions, "");
		while (iterator.hasNext()) {
			Object obj;
			obj = iterator.next();
			nCounter2++;
		}
		lTimeEnd = System.currentTimeMillis();

		lTimeWithBPlus = lTimeEnd - lTimeStart;

		System.out
				.println("testcase: testTableCreateInsert2000SelectBuildIndexSelect");
		System.out.println("             running first select in "
				+ lTimeWithoutBPlus + " ms -  returned  " + nCounter1
				+ " tuples ");
		System.out
				.println("             running second select (with B+ tree) in "
						+ lTimeWithBPlus
						+ " ms - returned "
						+ nCounter2
						+ " tuples ");
		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert250SelectWith2ConditionsAndedEmptyResultSet()
			throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals, htblConditions;
		int nCounter = 0;
		Iterator iterator;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 250; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			if (nIndex % 2 == 0)
				htblVals.put("Department", "Sales");
			else
				htblVals.put("Department", "Marketing");

			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));

			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}

		htblConditions = new Hashtable();
		htblConditions.put("Insured", "True");
		htblConditions.put("Department", "Marketing");

		iterator = app.selectRangeFromTable("Employee", htblConditions, "AND");
		while (iterator.hasNext()) {
			Object obj;
			obj = iterator.next();
			nCounter++;
		}

		System.out
				.println("testcase: testTableCreateInsert250SelectWith2ConditionsAndedEmptyResultSet");
		System.out.println("             running select returned: " + nCounter);

		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert250SelectWith2ConditionsAndedNotEmptyResultSet()
			throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals, htblConditions;
		int nCounter = 0;
		Iterator iterator;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 250; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			if (nIndex % 2 == 0)
				htblVals.put("Department", "Sales");
			else
				htblVals.put("Department", "Marketing");

			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));

			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}

		htblConditions = new Hashtable();
		htblConditions.put("Insured", "True");
		htblConditions.put("Department", "Sales");

		iterator = app.selectRangeFromTable("Employee", htblConditions, "AND");
		while (iterator.hasNext()) {
			Object obj;
			obj = iterator.next();
			nCounter++;
		}

		System.out
				.println("testcase: testTableCreateInsert250SelectWith2ConditionsAndedNotEmptyResultSet");
		System.out.println("             running select returned: " + nCounter);

		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert250SelectWith2ConditionsOredDifferentConditions()
			throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals, htblConditions;
		int nCounter = 0;
		Iterator iterator;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 250; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			if (nIndex % 2 == 0)
				htblVals.put("Department", "Sales");
			else
				htblVals.put("Department", "Marketing");

			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));

			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}

		htblConditions = new Hashtable();
		htblConditions.put("Insured", "True");
		htblConditions.put("Department", "Marketing");

		iterator = app.selectRangeFromTable("Employee", htblConditions, "OR");
		while (iterator.hasNext()) {
			Object obj;
			obj = iterator.next();
			nCounter++;
		}

		System.out
				.println("testcase: testTableCreateInsert250SelectWith2ConditionsOredDifferentConditions");
		System.out.println("             running select returned: " + nCounter);

		System.in.read();

		app.saveAll();
	}

	protected void testTableCreateInsert250SelectWith2ConditionsOredSameCondition()
			throws Exception {
		DBApp app;
		Hashtable htblCols, htblVals, htblConditions;
		int nCounter = 0;
		Iterator iterator;

		app = new DBApp();

		app.init();

		htblCols = new Hashtable();
		htblCols.put("ID", "Java.lang.Integer");
		htblCols.put("Name", "Java.lang.String");
		htblCols.put("Department", "Java.lang.String");
		htblCols.put("Salary", "Java.lang.Float");
		htblCols.put("Insured", "Java.lang.Boolean");

		app.createTable("Employee", htblCols, null, "ID");

		for (int nIndex = 0; nIndex < 250; nIndex++) {
			htblVals = new Hashtable();
			htblVals.put("ID", Integer.toString(nIndex + 1));
			htblVals.put("Name", "John" + Integer.toString(nIndex + 1));
			if (nIndex % 2 == 0)
				htblVals.put("Department", "Sales");
			else
				htblVals.put("Department", "Marketing");

			htblVals.put("Salary", Float.toString(nIndex + 1 * 10.0f));

			if (nIndex % 2 == 0)
				htblVals.put("Insured", "True");
			else
				htblVals.put("Insured", "False");

			app.insertIntoTable("Employee", htblVals);
		}

		htblConditions = new Hashtable();
		htblConditions.put("Insured", "True");
		htblConditions.put("Insured", "False");

		iterator = app.selectRangeFromTable("Employee", htblConditions, "OR");
		while (iterator.hasNext()) {
			Object obj;
			obj = iterator.next();
			nCounter++;
		}

		System.out.println("testcase: testTableCreateInsert250SelectWith2ConditionsOredSameCondition");
		System.out.println("             running select returned: " + nCounter);

		System.in.read();

		app.saveAll();
	}

	public void runTests() {
		try {
			 testTableCreate();
//			
//			 testTableCreateInsert1();
//			
//			 testTableCreateInsert100();
//			
//			 testTableCreateInsert450();
//			
//			 testTableCreateInsert250Delete125();
//			
//			 testTableCreateInsert250SelectDelete125Select();
//			
//			 testTableCreateInsert75BuildIndex();
//			
//			 testTableCreateInsert2000SelectBuildIndexSelect();
//			
//			 testTableCreateInsert250SelectWith2ConditionsAndedEmptyResultSet();
//			
//			 testTableCreateInsert250SelectWith2ConditionsAndedNotEmptyResultSet();

//			testTableCreateInsert250SelectWith2ConditionsOredDifferentConditions();

		} catch (Exception exp) {
			exp.printStackTrace();
		}

	}

	public static void main(String[] args) {
		DBAppTest app;
		app = new DBAppTest();
		app.runTests();
	}

}

class ExtensionFilter implements FilenameFilter {
	private String extension;

	public ExtensionFilter(String extension) {
		this.extension = extension;
	}

	public boolean accept(File dir, String name) {
		return (name.endsWith(extension));
	}

}