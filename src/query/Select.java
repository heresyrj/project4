package query;

import global.AttrType;
import global.Minibase;
import heap.HeapFile;
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
    private Schema tempSchema;
    private Iterator[] scans;
    private HashMap<String, Integer> joinMap;
    private ArrayList<Predicate> joinPreds;
    private ArrayList<Iterator> closescan;
    private boolean isExplain;
    private ArrayList<Predicate[]> genSelection;
    ArrayList<Integer> availtables;

    private class Pair{

        int product, t1, t2;
        Predicate p;

        Pair (int product, Predicate p, int t1, int t2){
            this.product = product;
            this.p = p;
            this.t1 = t1;
            this.t2 = t2;
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
        preds = tree.getPredicates();
        scans = new Iterator[tables.length];
        joinMap = new HashMap<String, Integer>();
        joinPreds = new ArrayList<Predicate>();
        closescan = new ArrayList<Iterator>();
        genSelection = new ArrayList<Predicate[]>();
        availtables = new ArrayList<Integer>();

        try {
            for (String t: tables){
                QueryCheck.tableExists(t);
            }
            if (tables.length > 1) {
                joinSchema(tables);
                QueryCheck.predicates(tempSchema, preds);
                QueryCheck.updateFields(tempSchema, projCols);
            }
            else{
                Schema schema = Minibase.SystemCatalog.getSchema(tables[0]);
                QueryCheck.updateFields(schema, projCols);
            }

            for (int i = 0; i < tables.length; i++){
                availtables.add(i);
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
        boolean flag, gen, join;
        Schema schema;
        HeapFile hf;
        for (int i = 0; i < preds.length; i++){
            gen = true;
            for (int k = 0; k < tables.length; k++){
                flag = true;
                schema = Minibase.SystemCatalog.getSchema(tables[k]);
                hf = new HeapFile(tables[k]);
                scans[k] = new FileScan(schema, hf);
                closescan.add(scans[k]);
                // map from field name to table index
                for (int m = 0; m < schema.getCount(); m++){
                    joinMap.put(schema.fieldName(m), k);
                }
                for (int j = 0; j < preds[i].length; j++){
                    // used later for join ordering
                    if (preds[i][j].getLtype() == AttrType.COLNAME && preds[i][j].getRtype() ==AttrType.COLNAME){
                        joinPreds.add(preds[i][j]);
                        join = false;
                    }

                    if (!preds[i][j].validate(schema)){
                        flag = false;
                    }
                }
                if (flag){
                    scans[k] = new Selection(scans[k], preds[i]);
                    gen = false;
                    break;
                }
            }
            if (gen){
                join = true;
                for (int j = 0; j < preds[i].length; j++) {
                    if (preds[i][j].getLtype() == AttrType.COLNAME && preds[i][j].getRtype() ==AttrType.COLNAME){
                        join = false;
                    }
                }
                if (join){
                    genSelection.add(preds[i]);
                }
//                genSelection.add(preds[i]);
            }
        }
    }


    private ArrayList<Pair> processJoinPreds(){
        Set<Predicate> s = new HashSet<Predicate>();
        s.addAll(joinPreds);
        joinPreds.clear();

        boolean flag;
        for (Predicate p: s){
            flag = true;
            for (Predicate match: joinPreds){
                if (p.getLeft().equals(match.getLeft()) && p.getRight().equals(match.getRight())){
                    flag = false;
                }
                if (p.getRight().equals(match.getLeft()) && p.getLeft().equals(match.getRight())){
                    flag = false;
                }
            }
            if (flag){
                joinPreds.add(p);
            }
        }



        HeapFile hf1, hf2;
        int t1, t2, product;
        ArrayList<Pair> sort = new ArrayList<Pair>();

        for (Predicate p: joinPreds){
            t1 = joinMap.get(p.getLeft());
            t2 = joinMap.get(p.getRight());
            hf1 = new HeapFile(tables[t1]);
            hf2 = new HeapFile(tables[t2]);
            product = hf1.getRecCnt()*hf2.getRecCnt();
            sort.add(new Pair(product, p, t1, t2));
        }

        Collections.sort(sort, new Comparator<Pair>() {
            @Override
            public int compare(Pair o1, Pair o2) {
                return o1.product - o2.product;
            }
        });

        return sort;
    }

    private Iterator joinOrdering(){
        ArrayList<Pair> sort = processJoinPreds();
        Iterator sj;
        if (sort.size() == 0){
            sj = scans[0];
            for (int i = 1; i< tables.length; i++){
                sj = new SimpleJoin(sj, scans[i]);
            }
            return sj;
        }

        Pair pair = sort.get(0);
        sj = new SimpleJoin(scans[pair.t1], scans[pair.t2], pair.p);
        closescan.add(sj);

        Schema temp;
        for (int i = 1; i < sort.size(); i++){
            pair = sort.get(i);
            for (int j = 0; j < tables.length; j++){
                temp = Schema.join(sj.getSchema(), scans[j].getSchema());
                if (pair.p.validate(temp)){
                    sj = new SimpleJoin(sj, scans[j], pair.p);
                    availtables.remove(j);
                    break;
                }
            }
        }

        int get;
        while (availtables.size() > 0){
            get = availtables.remove(0);
            sj = new SimpleJoin(sj, scans[get]);
        }

        return sj;
    }



    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {
        Iterator sj;
        ArrayList<String> availtables = new ArrayList<String>();

        if (preds.length == 0){
            Schema schema;
            HeapFile hf;
            for (int i = 0; i< tables.length; i++){
                schema = Minibase.SystemCatalog.getSchema(tables[i]);
                hf = new HeapFile(tables[i]);
                scans[i] = new FileScan(schema, hf);
                closescan.add(scans[i]);
            }
            sj = scans[0];
            for (int i = 1; i< tables.length; i++){
                sj = new SimpleJoin(sj, scans[i]);
            }

        }
        else {
            //Pushing Selections:
            pushingSelection();

//        Selection finalSel;

            if (tables.length > 1) {
                sj = joinOrdering();
            } else {
                sj = scans[0];
            }

            if (genSelection.size() > 0) {
                sj = new Selection(sj, genSelection.get(0));
//            closescan.add(sj);
                for (int i = 1; i < genSelection.size(); i++) {
                    sj = new Selection(sj, genSelection.get(i));
                }
            }
        }

        if (projCols.length != 0) {
            Schema schema = sj.getSchema();
            Integer[] pCols = new Integer[projCols.length];
            for (int i = 0; i < projCols.length; i++) {
                pCols[i] = schema.fieldNumber(projCols[i]);
            }
            Projection proj = new Projection(sj, pCols);
            closescan.add(proj);

            if (isExplain){
                proj.explain(0);
                System.out.println("");

            }
            proj.execute();

        }
        else{
            if (isExplain){
                sj.explain(0);
                System.out.println("");
            }
            sj.execute();

        }

        for (Iterator i: closescan){
            if (i.isOpen()){
                i.close();
            }
        }



//         print the output message
//        System.out.println("0 rows affected. ");

    } // public void execute()



} // class Select implements Plan
