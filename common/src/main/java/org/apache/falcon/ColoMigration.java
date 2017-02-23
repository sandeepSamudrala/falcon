package org.apache.falcon;

import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Clusters;
import org.apache.falcon.entity.v0.process.Validity;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

public class ColoMigration {
    public static void main(String[] args) {
        if(args.length != 3){
            System.out.println("Specify correct arguments");
            return;
        }
        String entitytype = args[0].trim().toLowerCase();
        String oldEntities = args[1];
        String outpath = args[2];
        changeEntities(entitytype, oldEntities, outpath);
    }

    public static void changeEntities(String entityType, String oldPath, String newPath) {
        File folder = new File(oldPath);
        File[] listOfFiles = folder.listFiles();

        for (File file : listOfFiles) {
            if (file.isFile()) {
                System.out.println(file.getName());
                EntityType type = EntityType.getEnum(entityType);
                EntityParser<?> entityParser = EntityParserFactory.getParser(type);
                try {
                    InputStream xmlStream = new FileInputStream(file);
                    File outputFile = new File(newPath + file.getName());
                    FileOutputStream outputStream = new FileOutputStream(outputFile);

                    switch (type) {
                        case PROCESS:
                            org.apache.falcon.entity.v0.process.Process entity =
                                    (org.apache.falcon.entity.v0.process.Process) entityParser.parse(xmlStream);

                            Clusters entityClusters = entity.getClusters();
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
                                    currentDate.setTime(1487894400);
                                    pek1Validity.setStart(currentDate);
                                    pek1Validity.setEnd(cluster.getValidity().getEnd());
                                    clusters.add(pek1_cluster);
                                    break;
                                }
                            }

                            type.getMarshaller().marshal(entity, outputStream);
                            outputStream.flush();
                            outputStream.close();

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
                                }
                            }

                            type.getMarshaller().marshal(feed, outputStream);
                            outputStream.flush();
                            outputStream.close();
                    }

                } catch (FileNotFoundException e) {

                } catch (FalconException e) {

                } catch (JAXBException e) {

                } catch (IOException e) {

                }
            }
        }
    }
}
