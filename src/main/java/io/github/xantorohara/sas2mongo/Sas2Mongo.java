package io.github.xantorohara.sas2mongo;

import au.com.bytecode.opencsv.CSVReader;
import com.ggasoftware.parso.Column;
import com.ggasoftware.parso.SasFileReader;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Sas2Mongo {
    protected static Logger log = LoggerFactory.getLogger(Sas2Mongo.class);

    private String[] columns;
    private Set<String> fields;

    private String file;
    private String collection;
    private String action;

    private String mongoDb;
    private String mongoHost;
    private int mongoPort;

    private DBCollection mongoCollection;

    public static void main(String[] args) throws IOException {
        Sas2Mongo sas2Mongo = new Sas2Mongo();
        if (!sas2Mongo.parseCommandLine(args)) {
            log.error("Invalid command line options");
            return;
        }
        sas2Mongo.process();
    }

    private static boolean isEmpty(String string) {
        return string == null || string.isEmpty();
    }

    private boolean parseCommandLine(String[] args) {
        Options options = new Options().
                addOption("i", "file", true, "Input sas file").
                addOption("o", "collection", true, "Output mongo collection").
                addOption("a", "action", true, "What to do if the collection is not empty (append or drop)").
                addOption("f", "fields", true, "Comma separated list of fields (all fields by default)").
                addOption("d", "mongo-db", true, "Mongo db").
                addOption("h", "mongo-host", true, "Mongo host (localhost by default)").
                addOption("p", "mongo-port", true, "Mongo port (27017 by default)");

        CommandLineParser parser = new PosixParser();

        try {
            CommandLine cli = parser.parse(options, args);
            file = cli.getOptionValue("file");
            String fieldsParam = cli.getOptionValue("fields");
            if (!isEmpty(fieldsParam)) {
                fields = new HashSet<>(Arrays.asList(fieldsParam.split(",")));
            }

            collection = cli.getOptionValue("collection");
            action = cli.getOptionValue("action");

            mongoDb = cli.getOptionValue("mongo-db");
            mongoHost = cli.getOptionValue("mongo-host", "localhost");
            mongoPort = Integer.parseInt(cli.getOptionValue("mongo-port", "27017"));

            for (String string : new String[]{file, collection, mongoDb}) {
                if (isEmpty(string)) {
                    throw new ParseException("Missed required option");
                }
            }
            return true;
        } catch (Exception e) {
            log.error(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("sas2mongo --file=FILE --collection=COLLECTION --mongo-db=DB", options);
            return false;
        }
    }

    private void process() throws IOException {
        log.info("Connecting mongo...");

        MongoClient mongoClient = new MongoClient(mongoHost, mongoPort);

        try {
            DB db = mongoClient.getDB(mongoDb);

            log.info("Getting collection...");
            mongoCollection = db.getCollection(collection);

            if (mongoCollection.count() > 0) {
                log.info("Collection exists and has " + mongoCollection.count() + " records");
                if ("drop".equals(action)) {
                    log.info("Dropping...");
                    mongoCollection.drop();

                } else if ("append".equals(action)) {
                    log.info("Appending...");
                } else {
                    log.error("Collection is not empty, but --action is not specified");
                    return;
                }
            }

            log.info("Processing...");
            if (file.toLowerCase().endsWith(".sas7bdat")) {
                int records = processSasFile();
                log.info("Processed records:" + records);
            } else if (file.toLowerCase().endsWith(".csv")) {
                int records = processCsvFile();
                log.info("Processed records:" + records);
            }
        } finally {
            mongoClient.close();
        }
    }

    /**
     * Copy rows from the SAS-file to the mongo collection
     */
    private int processSasFile() throws IOException {
        int records = 0;
        try (InputStream is = new FileInputStream(file)) {
            SasFileReader reader = new SasFileReader(is);

            List<Column> sasColumns = reader.getColumns();
            columns = new String[sasColumns.size()];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = sasColumns.get(i).getName();
            }
            strikeColumnsByFields();

            Object[] row;
            while ((row = reader.readNext()) != null) {
                mongoInsert(row);
                records++;
            }
        }
        return records;
    }

    /**
     * Copy rows from the CSV-file to the mongo collection
     */
    private int processCsvFile() throws IOException {
        int records = 0;
        try (FileReader fr = new FileReader(file)) {
            CSVReader reader = new CSVReader(fr);

            columns = reader.readNext();
            strikeColumnsByFields();

            String[] row;
            while ((row = reader.readNext()) != null) {
                mongoInsert(row);
                records++;
            }
        }
        return records;
    }

    /**
     * Retain only columns from the fields list, else replace column with null
     */
    private void strikeColumnsByFields() {
        if (fields != null) {
            for (int i = 0; i < columns.length; i++) {
                if (!fields.contains(columns[i])) {
                    columns[i] = null;
                }
            }
        }
        log.info("Columns to process: " + Arrays.toString(columns));
    }

    /**
     * Insert new document to the mongo collection
     *
     * @param row from table
     */
    private void mongoInsert(Object[] row) {
        BasicDBObject doc = new BasicDBObject();
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                doc.append(columns[i], row[i]);
            }
        }
        mongoCollection.insert(doc);
    }
}
