package query;

import global.AttrType;
import global.Minibase;
import global.SortKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.*;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

    /**
     * (EXPLAIN) SELECT sid, name, points
     * FROM Students, Grades
     * WHERE sid = gsid AND points >= 3.0 OR sid = gsid AND points <= 2.5;
     * */

    /**
     * The main goal of Select's constructor is to create an Iterator query tree. (i.e. all you
     * need to do in execute() is call iter.explain() or iter.execute())
     */

    private boolean isExplain;
    private SortKey[] orders; //ascending or descending order for given field
    private boolean kleenestar = false;
    private String[] tables;
    private Predicate[][] preds;
    private String[] projCols;
    private HashMap<String, String> col2Table;
    private Schema tempSchema;

    /**
     * Optimizes the plan, given the parsed query.
     *
     * @throws QueryException if validation fails
     */
    public Select(AST_Select tree) throws QueryException {
        isExplain = tree.isExplain;
        tables = tree.getTables();
        projCols = tree.getColumns();
        if(projCols.length == 0) kleenestar = true;
        preds = tree.getPredicates();
        try {
            for (String t: tables){
                QueryCheck.tableExists(t);
            }
            if (tables.length > 1) {
                joinSchema(tables);
            }
            validateProjCols();
            QueryCheck.predicates(tempSchema, preds);

        } catch (QueryException e) {
            throw e;
        }
    } // public Select(AST_Select tree) throws QueryException

    private void joinSchema(String[] tables){
        try {
            tempSchema = Schema.join(QueryCheck.tableExists(tables[0]), QueryCheck.tableExists(tables[1]));
            for (int i = 2; i < tables.length; i++){
                tempSchema = Schema.join(tempSchema, QueryCheck.tableExists(tables[i]));
            }
        } catch (QueryException e) {}
    }

    private class TableInfo {
        HeapFile origin;
        HeapFile newhf;
        Schema schema;
        FileScan scan;
        int recCount;

        TableInfo (Schema schema, HeapFile hf) {
            this.schema = schema;
            this.origin = hf;
            this.newhf = new HeapFile(null);
            this.scan = new FileScan(schema, hf);
            this.recCount = origin.getRecCnt();
        }

        int containCol (String col) {
            return schema.fieldNumber(col);
        }


        void pushSelection(Predicate pd) {
            Tuple tuple;
            while(scan.hasNext()) {
                tuple = scan.getNext();
                if(pd.evaluate(tuple)) {
                    newhf.insertRecord(tuple.getData());
                }
            }
        }

    }

    private String pushable(Predicate p) {
        String left = (String) p.getLeft();
        String leftTable = col2Table.get(left);

        if(p.getRtype() != AttrType.COLNAME) return leftTable;

        String right = (String) p.getRight();
        String rightTable = col2Table.get(right);

        if (leftTable.equals(rightTable)){
            return leftTable;
        }
        else
            return null;

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

        TableInfo tinfo;
        String tableName;
        ArrayList<Predicate> newPreds;
        //Pushing Selections:
        for(Predicate[] pred : preds){ //connected by OR
            for (Predicate p: pred){
                if(pushable(p) != null) {

                }
            }
            //update
        }

        //Join Ordering:



        // print the output message
        System.out.println("0 rows affected. (Not implemented)");

    } // public void execute()

    private void validateProjCols() throws QueryException {

        col2Table = new HashMap<String, String>();
        ArrayList<String> colNames = new ArrayList<String>();
        Schema schema;
        for (String table : tables) {
            schema = QueryCheck.tableExists(table);
            for (int j = 0; j < schema.getCount(); j++) {
                String thisCol = schema.fieldName(j);
                colNames.add(thisCol);
                col2Table.put(thisCol, table);
            }
        }

        for (String column : projCols) {
            if (!colNames.contains(column)) throw new QueryException("No Col Match");
        }

    }

} // class Select implements Plan
