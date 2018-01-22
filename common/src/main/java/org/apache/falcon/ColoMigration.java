package org.apache.falcon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.xml.bind.JAXBException;

import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.*;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.Path;

public class ColoMigration {
    private static final String TMP_BASE_DIR = String.format("file://%s", new Object[]{System.getProperty("java.io.tmpdir")});

    public static void main(String[] args)
            throws Exception {
        if (args.length != 5) {
            System.out.println("Specify correct arguments");
        }
        String entitytype = args[0].trim().toLowerCase();
        String oldEntities = args[1];
        String outpath = args[2];
        String newClusterName = args[3];
        String newColoName = args[4];
        changeEntities(entitytype, oldEntities, outpath, newClusterName, newColoName);
    }

    public static void changeEntities(String entityType, String oldPath, String newPath, String newClusterName,
                                      String newColoName) {
        File folder = new File(oldPath);
        File[] listOfFiles = folder.listFiles();
        System.out.println("Number of files: " + listOfFiles.length);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        for (File file : listOfFiles) {
            if (file.isFile()) {
                System.out.println(file.getName());
                EntityType type = EntityType.getEnum(entityType);
                EntityParser<?> entityParser = EntityParserFactory.getParser(type);
                try {
                    InputStream xmlStream = new FileInputStream(file);
                    OutputStream out;
                    switch (type) {
                        case PROCESS:
                            Process process = (Process) entityParser.parse(xmlStream);
                            org.apache.falcon.entity.v0.process.Clusters entityClusters = process.getClusters();
                            List<org.apache.falcon.entity.v0.process.Cluster> clusters = entityClusters.getClusters();

                            org.apache.falcon.entity.v0.process.Validity validity = new org.apache.falcon.entity.v0.process.Validity();

                            Date startDate= simpleDateFormat.parse("2017-12-22T00:00:00Z");
                            Date endDate = simpleDateFormat.parse("2099-12-20T00:00:00Z");
                            validity.setStart(startDate);
                            validity.setEnd(endDate);


                            org.apache.falcon.entity.v0.process.Cluster processClusterToAdd = new org.apache.falcon.entity.v0.process.Cluster();

                            for (org.apache.falcon.entity.v0.process.Cluster cluster : clusters) {
                                if (cluster.getName().equals("lhr1-emerald")) {
                                    processClusterToAdd.setValidity(validity);
                                    processClusterToAdd.setSla(cluster.getSla());
                                    processClusterToAdd.setName(newClusterName);
                                }
                            }


                            process.getClusters().getClusters().add(processClusterToAdd);

                            // filter on start date for processes
                            boolean filter = false;
                            List<String> processClusterNames = new ArrayList<>();
                            for (org.apache.falcon.entity.v0.process.Cluster cluster : clusters) {
                                Date clusterDate = cluster.getValidity().getEnd();
                                processClusterNames.add(cluster.getName());
                                if (clusterDate.getTime() > System.currentTimeMillis() ) {
                                    filter = true;
                                }
                            }

                            if(processClusterNames.size() != 0) {
                                processClusterNames.add("prism");
                            } else {
                                System.out.println("process to delete: " + file.getAbsolutePath());
                            }

                            if (filter) {
                                for (String colo : processClusterNames) {
                                    File entityFile = new File(new Path(newPath + File.separator + colo + File.separator +
                                            file.getName()).toUri().toURL().getPath());
                                    entityFile.getParentFile().mkdirs();
                                    System.out.println("File path : " + entityFile.getAbsolutePath());
                                    if (!entityFile.createNewFile()) {
                                        System.out.println("Not able to stage the entities in the tmp path");
                                        return;
                                    }
                                    out = new FileOutputStream(entityFile);
                                    type.getMarshaller().marshal(process, out);
                                    out.close();
                                }
                            }

                            break;


                        case FEED:
                            Feed feed = (Feed) entityParser.parse(xmlStream);

                            org.apache.falcon.entity.v0.feed.Clusters feedClusters = feed.getClusters();
                            List<org.apache.falcon.entity.v0.feed.Cluster> feed_clusters = feedClusters.getClusters();

                            startDate= simpleDateFormat.parse("2017-11-20T00:00:00Z");
                            endDate = simpleDateFormat.parse("2099-11-20T00:00:00Z");
                            org.apache.falcon.entity.v0.feed.Validity feedValidity = new org.apache.falcon.entity.v0.feed.Validity();

                            feedValidity.setStart(startDate);
                            feedValidity.setEnd(endDate);

                            Partition newPartition = new Partition();
                            newPartition.setName(newColoName);

                            boolean ignoreFeed = false;
                            org.apache.falcon.entity.v0.feed.Cluster feedClusterToAdd = new Cluster();
                            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                if (cluster.getName().equals("ams1-azurite")) {
                                    ignoreFeed = true;
                                }
                            }

                            if (!ignoreFeed) {
                                for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                    if (cluster.getName().equals("lhr1-emerald")) {
                                        feedClusterToAdd.setType(cluster.getType());
                                        feedClusterToAdd.setValidity(feedValidity);
                                        feedClusterToAdd.setRetention(cluster.getRetention());
                                        if (cluster.getPartition() != null && cluster.getPartition().equals("lhr1")) {
                                            feedClusterToAdd.setPartition(newColoName);
                                        }
                                        feedClusterToAdd.setLifecycle(cluster.getLifecycle());
                                        feedClusterToAdd.setDelay(cluster.getDelay());
                                        feedClusterToAdd.setSla(cluster.getSla());
                                        feedClusterToAdd.setName(newClusterName);
                                        feedClusterToAdd.setPartition(cluster.getPartition());

                                        if (cluster.getLocations() != null && cluster.getLocations().getLocations() != null) {
                                            String oldLocation = cluster.getLocations().getLocations().get(0).getPath();
                                            Locations feedLocations = new Locations();
                                            Location newLocation = new Location();
                                            String newLocationPath = oldLocation.replace("lhr1", newColoName)
                                                    .replace("Lhr1", "Ams1")
                                                    .replace("LHR1", "AMS1");
                                            newLocation.setPath(newLocationPath);
                                            newLocation.setType(cluster.getLocations().getLocations().get(0).getType());
                                            feedLocations.getLocations().add(newLocation);
                                            feedClusterToAdd.setLocations(feedLocations);
                                        }
                                    }
                                }



                                feed.getClusters().getClusters().add(feedClusterToAdd);

                                List<String> feedClusterNames = new ArrayList<>();
                                for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                    feedClusterNames.add(cluster.getName());
                                }

                                if(feedClusterNames.size() != 0) {
                                    feedClusterNames.add("prism");
                                } else {
                                    System.out.println("Feed to delete: " + file.getAbsolutePath());
                                }
                                for (String colo : feedClusterNames) {
                                    File entityFile = new File(new Path(newPath + File.separator + colo + File.separator
                                            + file.getName()).toUri().toURL().getPath());
                                    entityFile.getParentFile().mkdirs();
                                    System.out.println("File path : " + entityFile.getAbsolutePath());
                                    if (!entityFile.createNewFile()) {
                                        System.out.println("Not able to stage the entities in the tmp path");
                                        return;
                                    }
                                    out = new FileOutputStream(entityFile);
                                    type.getMarshaller().marshal(feed, out);
                                    out.close();
                                }
                            }
                    }

                } catch (FileNotFoundException | FalconException | JAXBException e) {
                    System.out.println(e.toString());
                } catch (IOException | ParseException e) {
                    e.printStackTrace();
                    System.out.println(e.toString());
                }
            }
        }
    }
}
