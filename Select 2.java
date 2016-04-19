package query;

import global.AttrType;
import global.Minibase;
import global.RID;
import global.SortKey;
import heap.HeapFile;
import heap.HeapScan;
import parser.AST_Select;
import relop.*;
import relop.Iterator;

import java.util.*;


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

    private String[] tables;
    private Predicate[][] preds;
    private String[] projCols;
    private ArrayList<String> joinedTables;
    private Schema tempSchema;
    private Iterator[] scans;
    private HashMap<String, Integer> joinMap;
    private Predicate[][] joinMatrix;
    private ArrayList<Predicate> joinPreds;

    private class Pair{

        int count;
        int table;

        Pair (int count, int table){
            this.count = count;
            this.table = table;
        }


    }


    /**
     * Optimizes the plan, given the parsed query.
     *
     * @throws QueryException if validation fails
     */
    public Select(AST_Select tree) throws QueryException {
        tables = tree.getTables();
        projCols = tree.getColumns();
        preds = tree.getPredicates();
        joinedTables = new ArrayList<String>();
        scans = new Iterator[tables.length];
        joinMap = new HashMap<String, Integer>();
        joinMatrix = new Predicate[tables.length][tables.length];
        joinPreds = new ArrayList<Predicate>();
        try {
            for (String t: tables){
                QueryCheck.tableExists(t);
            }
            if (tables.length > 1) {
                joinSchema(tables);
                QueryCheck.predicates(tempSchema, preds);
            }

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

    private void pushingSelection(){
        boolean flag = true;
        Schema schema;
        HeapFile hf;
        for (int i = 0; i < preds.length; i++){
            for (int k = 0; k < tables.length; k++){
                schema = Minibase.SystemCatalog.getSchema(tables[k]);
                hf = new HeapFile(tables[k]);
                scans[k] = new FileScan(schema, hf);
                // map from field name to table index
                for (int m = 0; m < schema.getCount(); m++){
                    joinMap.put(schema.fieldName(m), k);
                }
                for (int j = 0; j < preds[i].length; j++){
                    // used later for join ordering
                    if (preds[i][j].getLtype() == AttrType.COLNAME && preds[i][j].getRtype() ==AttrType.COLNAME){
                        joinPreds.add(preds[i][j]);
                    }

                    if (!preds[i][j].validate(schema)){
                        flag = false;
                    }
                }
                if (flag){
                    scans[k] = new Selection(scans[k], preds[i]);
                    break;
                }
            }
        }
    }

    private Predicate[] findJoinPred(Integer left, Integer right){
        Predicate[] res = new Predicate[1];

        for (Predicate p: joinPreds){
            int leftT = joinMap.get(p.getLeft());
            int rightT = joinMap.get(p.getRight());
            if (left == null && rightT == right){
                res[0] = p;
                break;
            }
            else if (left == null && rightT == left){
                res[0] = p;
                break;
            }
            else if (leftT == left && rightT == right){
                res[0] = p;
                break;
            }
            else if (leftT == right && rightT == left){
                res[0] = p;
                break;
            }
        }
        if (res[0] != null){
            joinPreds.remove(res[0]);
            return res;
        }
        return null;
    }

    private void removeDupJoinPreds(){
        Set<Predicate> s = new HashSet<Predicate>();
        s.addAll(joinPreds);
        joinPreds.clear();
        joinPreds.addAll(s);
    }

    private Iterator joinOrdering(){
        removeDupJoinPreds();
        Iterator sj;
        HeapFile hf;
        ArrayList<Pair> sort = new ArrayList<Pair>();
        for (int i = 0; i < tables.length; i++){
            hf = new HeapFile(tables[i]);
            sort.add(new Pair(hf.getRecCnt(), i));
        }

        Collections.sort(sort, new Comparator<Pair>() {
            @Override
            public int compare(Pair o1, Pair o2) {
                return o1.count - o2.count;
            }
        });

        Integer left = sort.get(0).table;
        Integer right = sort.get(1).table;

        sj = new SimpleJoin(scans[left], scans[right], findJoinPred(left, right));

        for (int i = 2; i < sort.size(); i++){
            right = sort.get(0).table;

            sj = new SimpleJoin(sj, scans[right], findJoinPred(null, right));
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
            sj = scans[0];
        }

        finalSel = new Selection(sj, preds[0]);
        for (Predicate[] pred : preds) {
            if(pred.equals(preds[0])) continue;
            finalSel = new Selection(finalSel, pred);
        }

        if (projCols.length != 0) {
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
//        System.out.println("0 rows affected. ");

    } // public void execute()



} // class Select implements Plan
