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
    private HashMap<String, TableInfo> info;
    ArrayList<ArrayList<Predicate>> unPushablePreds;
    ArrayList<ArrayList<Predicate>> pushablePreds;
    private Schema tempSchema;

    private class TableInfo {
        String name;
        HeapFile origin, updated, temp;
        Schema schema;
        FileScan scan;

        TableInfo (String name, Schema schema, HeapFile hf) {
            this.name = name;
            this.schema = schema;
            this.origin = hf;
            this.updated = hf;
            this.temp = new HeapFile(null);
            this.scan = new FileScan(schema, hf);
        }

        int getCount () {
            return updated.getRecCnt();
        }

        FileScan getScan() {
            return new FileScan(schema, updated);
        }

        void pushSelection(Selection sel) {
            Tuple tuple;
            while(sel.hasNext()) {
                tuple = sel.getNext();
                temp.insertRecord(tuple.getData());
            }
            //update
            updated = temp;
            temp = new HeapFile(null);
        }

    }

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
        preprocessing();

    } // public Select(AST_Select tree) throws QueryException

    private void joinSchema(String[] tables){
        try {
            tempSchema = Schema.join(QueryCheck.tableExists(tables[0]), QueryCheck.tableExists(tables[1]));
            for (int i = 2; i < tables.length; i++){
                tempSchema = Schema.join(tempSchema, QueryCheck.tableExists(tables[i]));
            }
        } catch (QueryException e) {}
    }

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

    private boolean pushableForATable(Predicate p, String name) {
        String left = (String) p.getLeft();
        String leftTable = col2Table.get(left);

        if(p.getRtype() != AttrType.COLNAME && leftTable.equals(name)) return true;

        String right = (String) p.getRight();
        String rightTable = col2Table.get(right);

        if (leftTable.equals(rightTable) && leftTable.equals(name)){
            return true;
        }
        else
            return false;

    }

    private void preprocessing() {

        TableInfo tableInfo;
        info = new HashMap<String, TableInfo>();
        for(String table : tables) {
            tableInfo = new TableInfo(table, Minibase.SystemCatalog.getSchema(table) , new HeapFile(table));
            info.put(table, tableInfo);
        }

        predsAnalysis();
    }

    private void predsAnalysis() {
        unPushablePreds = new ArrayList<ArrayList<Predicate>>();
        pushablePreds = new ArrayList<ArrayList<Predicate>>();
        ArrayList<Predicate> push, notpush;

        boolean pushed = false;
        //determine if a Predicate[] involves only 1 table
        for (Predicate[] pred : preds) {
            push = new ArrayList<Predicate>();
            notpush = new ArrayList<Predicate>();

            for(Predicate p : pred) {
                //for each predicate, if find any table pushable for it.
                for(String t : tables) {
                    if(pushableForATable(p,t)) {
                        push.add(p);
                        pushed = true;
                        break;
                    }
                }
                //if no table found. add to notpush
                if(!pushed) notpush.add(p);
            }

            pushablePreds.add(push);
            unPushablePreds.add(notpush);
        }

    }

    private void pushingSelection() {
        //In each ArrayList<Predicate>, predicates are connected by OR
        //find longest Predicate[] for tables and do pushing selection
        ArrayList<Predicate> p4atable;
        TableInfo tinfo;
        Selection sel;

        for (ArrayList<Predicate> push : pushablePreds) {
            while(push.size() != 0) {
                for(String table : tables) {
                    p4atable = new ArrayList<Predicate>();
                    //this for loop find the longest OR chain predicates for current table
                    for(Predicate p : push) {
                        if(pushableForATable(p, table)) {
                            p4atable.add(p);
                            push.remove(p);
                        }
                    }
                    //if any predicates found for this table, pushing selection
                    if(p4atable.size() != 0) {
                        tinfo = info.get(table);
                        sel = new Selection(tinfo.scan, (Predicate[])p4atable.toArray());
                        tinfo.pushSelection(sel);
                    }

                }
            }
        }

    }


    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {

        //Pushing Selections:
        pushingSelection();

        //Join Ordering:
        HashMap<Integer, FileScan> size2table = new HashMap<Integer, FileScan>();
        for (TableInfo t : info.values()) {
            size2table.put(t.getCount(), t.getScan());
        }
        //sort by size
        Integer[] sortedSize = (Integer[]) size2table.keySet().toArray();

        //SimpleJoin sj = new SimpleJoin(size2table.get(sortedSize[0]), size2table.get(sortedSize[1]), );


        // print the output message
        System.out.println("0 rows affected. (Not implemented)");

    } // public void execute()



} // class Select implements Plan
