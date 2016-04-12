package query;

import global.Minibase;
import global.SortKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

    /**
     * SELECT sid, name, points
     * FROM Students, Grades
     * WHERE sid = gsid AND points >= 3.0 OR sid = gsid AND points <= 2.5;
     * */

    /**
     * The main goal of Select's constructor is to create an Iterator query tree. (i.e. all you
     * need to do in execute() is call iter.explain() or iter.execute())
     */

    private boolean isDistinct;
    private boolean isExplain;
    private String[] tables;
    private SortKey[] orders; //ascending or descending order for given field
    private Predicate[][] preds;
    private String[] columns;
    private boolean kleenestar = false;

    /**
     * Optimizes the plan, given the parsed query.
     *
     * @throws QueryException if validation fails
     */
    public Select(AST_Select tree) throws QueryException {
        isDistinct = tree.isDistinct;
        isExplain = tree.isExplain;
        tables = tree.getTables();
        columns = tree.getColumns();
        try {
            validate();
        } catch (QueryException e) {
            throw e;
        }
        if(columns.length == 0) kleenestar = true;
        orders = tree.getOrders();
        preds = tree.getPredicates();
    } // public Select(AST_Select tree) throws QueryException

    private void validate() throws QueryException {

        ArrayList<String> colNames = new ArrayList<String>();
        Schema schema;
        for (String table : tables) {
            schema = QueryCheck.tableExists(table);
            for (int j = 0; j < schema.getCount(); j++) {
                colNames.add(schema.fieldName(j));
            }
        }

        for (String column : columns) {
            if (!colNames.contains(column)) throw new QueryException("No Col Match");
        }

    }

    private class TableInfo {
        HeapFile origin;
        HeapFile hf;
        Schema schema;
        FileScan scan;
        int recCount;

        TableInfo (Schema schema, HeapFile hf) {
            this.schema = schema;
            this.origin = hf;
            this.hf = new HeapFile(null);
            this.scan = new FileScan(schema, hf);
            this.recCount = origin.getRecCnt();
        }

        int containCol (String col) {
            return schema.fieldNumber(col);
        }

    }


    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {
        TableInfo tableInfo;
        HashMap<String, TableInfo> info = new HashMap<String, TableInfo>();
        for(String table : tables) {
            tableInfo = new TableInfo(Minibase.SystemCatalog.getSchema(table) , new HeapFile(table));
            info.put(table, tableInfo);
        }

        //Pushing Selections:
        for (Predicate[] pred : preds) {
            for (Predicate pd : pred) {

            }
        }

        //Join Ordering:




        // print the output message
        System.out.println("0 rows affected. (Not implemented)");

    } // public void execute()

} // class Select implements Plan
