package org.apache.falcon;

import org.apache.commons.codec.CharEncoding;
import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Clusters;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.hadoop.fs.Path;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.Date;
import java.util.List;

public class ColoMigration {
    private static final String TMP_BASE_DIR = String.format("file://%s", System.getProperty("java.io.tmpdir"));
    private static final String UTF_8 = CharEncoding.UTF_8;
    public static void main(String[] args) throws Exception{
        if(args.length != 3){
            System.out.println("Specify correct arguments");
            //return;
        }
        String entitytype = args[0].trim().toLowerCase();
        String oldEntities = args[1];
        String outpath = args[2];
        changeEntities(entitytype, oldEntities, outpath);
        //changeEntities("process", "/home/pracheer/work/entities", "file:///home/pracheer/work/entities_n");

    }

    public static void changeEntities(String entityType, String oldPath, String newPath) throws Exception {
        File folder = new File(oldPath);
        File[] listOfFiles = folder.listFiles();
        String stagePath = TMP_BASE_DIR + File.separator + newPath + File.separator + System.currentTimeMillis()/1000;
        System.out.println("Number of files: " + listOfFiles.length);

        for (File file : listOfFiles) {

            if (file.isFile()) {
                System.out.println(file.getName());
                EntityType type = EntityType.getEnum(entityType);
                EntityParser<?> entityParser = EntityParserFactory.getParser(type);
                try {
                    InputStream xmlStream = new FileInputStream(file);
                    //PrintWriter out = new PrintWriter(file.getName());
                    File entityFile;
                    OutputStream out;
                    switch (type) {
                        case PROCESS:
                            org.apache.falcon.entity.v0.process.Process process =
                                    (org.apache.falcon.entity.v0.process.Process) entityParser.parse(xmlStream);

                            Clusters entityClusters = process.getClusters();
                            List<Cluster> clusters = entityClusters.getClusters();

                            for (Cluster cluster : clusters) {
                                if (cluster.getName().equals("hkg1-opal")) {
                                    Cluster pek1_cluster = new Cluster();
                                    pek1_cluster.setName("pek1-pyrite");
                                    if(cluster.getSla() != null) {
                                        pek1_cluster.setSla(cluster.getSla());
                                    }
                                    Validity pek1Validity = new Validity();
                                    Date currentDate = new Date();
                                    currentDate.setTime(1487894400000L);
                                    pek1Validity.setStart(currentDate);
                                    pek1Validity.setEnd(cluster.getValidity().getEnd());
                                    pek1_cluster.setValidity(pek1Validity);
                                    clusters.add(pek1_cluster);
                                    break;
                                }
                            }

                            entityFile = new File(new Path(newPath + File.separator + file.getName()).toUri().toURL().getPath());
                            System.out.println("File path : " + entityFile.getAbsolutePath());
                            if (!entityFile.createNewFile()) {
                                System.out.println(("Not able to stage the entities in the tmp path"));
                                return;
                            }

                            System.out.println(process.toString());
                            out = new FileOutputStream(entityFile);
                            type.getMarshaller().marshal(process, out);
                            out.close();
                            break;

                        case FEED:
                            org.apache.falcon.entity.v0.feed.Feed feed =
                                    (org.apache.falcon.entity.v0.feed.Feed) entityParser.parse(xmlStream);

                            org.apache.falcon.entity.v0.feed.Clusters feedClusters = feed.getClusters();
                            List<org.apache.falcon.entity.v0.feed.Cluster> feed_clusters = feedClusters.getClusters();

                            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                if (cluster.getName().equals("hkg1-opal")) {
                                    org.apache.falcon.entity.v0.feed.Cluster pek1_feed_cluster = new org.apache.falcon.entity.v0.feed.Cluster();
                                    pek1_feed_cluster.setName("pek1-pyrite");
                                    pek1_feed_cluster.setSla(cluster.getSla());
                                    pek1_feed_cluster.setDelay(cluster.getDelay());
                                    pek1_feed_cluster.setLifecycle(cluster.getLifecycle());
                                    pek1_feed_cluster.setLocations(cluster.getLocations());
                                    pek1_feed_cluster.setPartition(cluster.getPartition());
                                    pek1_feed_cluster.setRetention(cluster.getRetention());
                                    pek1_feed_cluster.setType(cluster.getType());

                                    org.apache.falcon.entity.v0.feed.Validity pek1Validity = new org.apache.falcon.entity.v0.feed.Validity();
                                    Date currentDate = new Date();
                                    currentDate.setTime(1487894400);
                                    pek1Validity.setStart(currentDate);
                                    pek1Validity.setEnd(cluster.getValidity().getEnd());
                                    pek1_feed_cluster.setValidity(pek1Validity);
                                    feed_clusters.add(pek1_feed_cluster);
                                    break;
                                }
                            }
                            entityFile = new File(new Path(newPath + File.separator + file.getName()).toUri().toURL().getPath());
                            System.out.println("File path : " + entityFile.getAbsolutePath());
                            if (!entityFile.createNewFile()) {
                                System.out.println(("Not able to stage the entities in the tmp path"));
                                return;
                            }

                            System.out.println(feed.toString());
                            out = new FileOutputStream(entityFile);
                            type.getMarshaller().marshal(feed, out);
                            out.close();
                            break;

                    }

                } catch (FileNotFoundException e) {
                    System.out.println(e.toString());
                } catch (FalconException e) {
                    System.out.println(e .toString());
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (JAXBException e) {
                    System.out.println(e.toString());
                } catch (Exception e) {
                    System.out.println(e.toString());
                }
            }
        }
    }
}
