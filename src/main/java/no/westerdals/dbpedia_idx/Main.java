package no.westerdals.dbpedia_idx;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("" +
                "Usage: TitleIndexer <input_path_to_nt_file> <options>" +
                "-s, import to solr" +
                "-m, import to a mongodb");
        /*
        final Stopwatch stopwatch = Stopwatch.createStarted();
        //new DBPediaLabelParser(Environment.getInputFile("labels")).importLabelsToMongo();
        new DBPediaShortAbstractIndexer(Environment.getInputFile("short_abstracts")).index();
        //new DBPediaCategoriesIndexer(Environment.getInputFile("article_categories")).index();
        //new DBPediaTypesIndexer(Environment.getInputFile("instance_types")).index();
        //search();
        System.out.println("took:"+ NumberFormat.getInstance().format(stopwatch.elapsed(TimeUnit.SECONDS))+" sec");
        System.out.println("done at "+new java.util.Date());*/
    }

}
