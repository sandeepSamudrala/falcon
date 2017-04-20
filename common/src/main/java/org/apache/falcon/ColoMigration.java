package org.apache.falcon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import javax.xml.bind.JAXBException;

import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.Path;

public class ColoMigration {
    private static final String TMP_BASE_DIR = String.format("file://%s", new Object[]{System.getProperty("java.io.tmpdir")});

    public static void main(String[] args)
            throws Exception {
        if (args.length != 3) {
            System.out.println("Specify correct arguments");
        }
        String entitytype = args[0].trim().toLowerCase();
        String oldEntities = args[1];
        String outpath = args[2];
        changeEntities(entitytype, oldEntities, outpath);
    }

    public static void changeEntities(String entityType, String oldPath, String newPath)
            throws Exception {
        File folder = new File(oldPath);
        File[] listOfFiles = folder.listFiles();
        String stagePath = TMP_BASE_DIR + File.separator + newPath + File.separator + System.currentTimeMillis() / 1000L;
        System.out.println("Number of files: " + listOfFiles.length);
        for (File file : listOfFiles) {
            if (file.isFile()) {
                System.out.println(file.getName());
                EntityType type = EntityType.getEnum(entityType);
                EntityParser<?> entityParser = EntityParserFactory.getParser(type);
                try {
                    InputStream xmlStream = new FileInputStream(file);
                    org.apache.falcon.entity.v0.process.Validity pek1Validity;
                    OutputStream out;
                    switch (type) {
                        case PROCESS:
                            Process process = (Process) entityParser.parse(xmlStream);
                            org.apache.falcon.entity.v0.process.Clusters entityClusters = process.getClusters();
                            List<org.apache.falcon.entity.v0.process.Cluster> clusters = entityClusters.getClusters();

                            List<org.apache.falcon.entity.v0.process.Cluster> processClusterToRemove = new ArrayList<>();
                            for (org.apache.falcon.entity.v0.process.Cluster cluster : clusters) {
                                if (cluster.getName().equals("hkg1-opal") || cluster.getName().equals("uj1-topaz") || cluster.getName().equals("wc1-ssp")) {
                                    processClusterToRemove.add(cluster);
                                }
                            }
                            clusters.removeAll(processClusterToRemove);

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

                            List<org.apache.falcon.entity.v0.feed.Cluster> feedClusterToRemove = new ArrayList<>();
                            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                if (cluster.getName().equals("hkg1-opal") || cluster.getName().equals("uj1-topaz")
                                        || cluster.getName().equals("wc1-ssp") || cluster.getName().equals("hkg1-opal-secondary")) {
                                    feedClusterToRemove.add(cluster);
                                }
                            }
                            feed_clusters.removeAll(feedClusterToRemove);

                            List<String> feedClusterNames = new ArrayList<>();
                            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                feedClusterNames.add(cluster.getName());
                            }
                            if(feedClusterNames.size() != 0) {
                                feedClusterNames.add("prism");
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
                } catch (FileNotFoundException e) {
                    System.out.println(e.toString());
                } catch (FalconException e) {
                    System.out.println(e.toString());
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
