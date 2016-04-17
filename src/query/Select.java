package query;

import global.AttrType;
import global.Minibase;
import global.RID;
import global.SortKey;
import heap.HeapFile;
import parser.AST_Select;
import relop.*;

import java.util.ArrayList;
import java.util.Arrays;
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
    private ArrayList<String> joinedTables;
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
//            this.temp = new HeapFile(null);
            this.scan = new FileScan(schema, hf);
        }

        int getCount () {
            return updated.getRecCnt();
        }

        FileScan getScan() {
            return new FileScan(schema, updated);
        }

        void pushSelection(Selection sel) {

            temp = new HeapFile(null);
//            int tempCount = temp.getRecCnt();
            Tuple tuple;
            while(sel.hasNext()) {
                tuple = sel.getNext();
                temp.insertRecord(tuple.getData());
            }
            //update
            updated = temp;
//            temp = new HeapFile(null);
//            tempCount = temp.getRecCnt();
//            int updateCount = updated.getRecCnt();
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
        joinedTables = new ArrayList<String>();
        try {
            for (String t: tables){
                QueryCheck.tableExists(t);
            }
            if (tables.length > 1) {
                joinSchema(tables);
                QueryCheck.predicates(tempSchema, preds);
            }
            validateProjCols();

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

        if(p.getRtype() != AttrType.COLNAME ) {
            if (leftTable.equals(name)){
                return true;
            }
            return false;
        }

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

            if (push.size() != 0) {
                pushablePreds.add(push);
            }
            if (notpush.size() != 0) {
                unPushablePreds.add(notpush);
            }
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
                            //push.remove(p); taken care in next if loop. concurrent issue otherwise.
                        }
                    }
                    if (p4atable.size() != 0){
                        for (Predicate p: p4atable){
                            push.remove(p);
                        }
                    }
                    //if any predicates found for this table, pushing selection
                    if(p4atable.size() != 0) {
                        tinfo = info.get(table);
//                        Predicate[] pass = p4atable.toArray(new Predicate[p4atable.size()]);
                        Predicate[] pass = new Predicate[p4atable.size()];
                        Predicate p, copyfrom;
                        for (int i = 0; i < p4atable.size(); i++){
                            copyfrom = p4atable.get(i);
                            p = new Predicate(copyfrom.getOper(), copyfrom.getLtype(), copyfrom.getLeft(), copyfrom.getRtype(), copyfrom.getRight());
                            pass[i] = p;
                        }
                        sel = new Selection(tinfo.scan, pass);
                        tinfo.pushSelection(sel);
                        int i = 0;
                    }

                }
            }
        }


//        fixDup();

    }

    private void fixDup(){
        TableInfo t;
        String name;
        HeapFile temp;
        RID rid;
        FileScan scan;
        ArrayList<RID> rids;
        for (String s:tables){
            rids = new ArrayList<RID>();
            t = info.get(s);
            name = t.name;
            scan = t.getScan();
            temp = new HeapFile(null);
            Tuple tuple;
            while(scan.hasNext()){
                tuple = scan.getNext();
                rid = scan.getLastRID();
                if (! rids.contains(rid)) {
                    temp.insertRecord(tuple.getData());
                    rids.add(rid);
                }
            }
            int c = t.updated.getRecCnt();
            c = temp.getRecCnt();
            t.updated = temp;
            c = t.updated.getRecCnt();
        }

    }

    private boolean joinable(TableInfo left, TableInfo right, Predicate p){
        String leftName = (String) p.getLeft();
        String leftTable = col2Table.get(leftName);
        String rightName = (String) p.getRight();
        String rightTable = col2Table.get(rightName);

        if (left == null){
            if (joinedTables.contains(leftTable) && right.name.equals(rightTable)){
                    return true;
            }
            if (joinedTables.contains(rightTable) && right.name.equals(leftTable)){
                    return true;
            }
        }

        if (leftTable.equals(left.name) && rightTable.equals(right.name)){
            return true;
        }
        if (leftTable.equals(right.name) && rightTable.equals(left.name)){
            return true;
        }
        return false;
    }



    private SimpleJoin joinTwoTables(TableInfo left, TableInfo right, SimpleJoin sj){
        Predicate[] pass;
        ArrayList<Predicate> joinOR;

        for (ArrayList<Predicate> pred: unPushablePreds){
            joinOR = new ArrayList<Predicate>();
            for (Predicate p: pred){
                if (joinable(left, right, p)){
                    joinOR.add(p);
                    //pred.remove(p);
                }
            }
            if (joinOR.size() != 0){
                for (Predicate p: joinOR){
                    pred.remove(p);
                }
            }

            if (joinOR.size() != 0){
                pass = (Predicate[]) joinOR.toArray(new Predicate[joinOR.size()]);
                if (left == null){
                    sj = new SimpleJoin(sj, right.getScan(), pass);
                }
                else {
                    sj = new SimpleJoin(left.getScan(), right.getScan(), pass);
                }
            }
        }
        return sj;
    }

    private SimpleJoin joinOrdering(){
        HashMap<Integer, TableInfo> size2table = new HashMap<Integer, TableInfo>();
        for (TableInfo t : info.values()) {
            size2table.put(t.getCount(), t);
        }
        //sort by size
        Integer[] sortedSize = (Integer[]) size2table.keySet().toArray(new Integer[size2table.keySet().size()]);
        Arrays.sort(sortedSize);

        TableInfo leftTInfo = size2table.get(sortedSize[0]);
        TableInfo rightTInfo = size2table.get(sortedSize[1]);

        SimpleJoin sj = joinTwoTables(leftTInfo, rightTInfo, null);

        size2table.remove(sortedSize[0]);
        size2table.remove(sortedSize[1]);
        joinedTables.add(leftTInfo.name);
        joinedTables.add(rightTInfo.name);

        int counter = 2;
        while (size2table.size() != 0){
            rightTInfo = size2table.get(sortedSize[counter]);
            counter++;
            sj = joinTwoTables(null, rightTInfo, sj);
            joinedTables.add(rightTInfo.name);
        }
        return sj;

    }


    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {

        //Pushing Selections:
        pushingSelection();

        Selection finalSel;
        Iterator sj;
        if (tables.length > 1) {
            sj = joinOrdering();
        } else {
            sj = info.get(tables[0]).getScan();
        }

        finalSel = new Selection(sj, preds[0]);
        for (Predicate[] pred : preds) {
            if(pred.equals(preds[0])) continue;
            finalSel = new Selection(finalSel, pred);
        }

        if (kleenestar == false) {
            Schema schema = finalSel.getSchema();
            Integer[] pCols = new Integer[projCols.length];
            for (int i = 0; i < projCols.length; i++) {
                pCols[i] = schema.fieldNumber(projCols[i]);
            }
            Projection proj = new Projection(finalSel, pCols);

            proj.execute();
        }
        else{
            finalSel.execute();
        }

        // print the output message
        System.out.println("0 rows affected. ");

    } // public void execute()



} // class Select implements Plan
