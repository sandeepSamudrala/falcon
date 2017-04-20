package org.apache.falcon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import javax.xml.bind.JAXBException;
import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.Path;

public class ColoT
{
    private static final String TMP_BASE_DIR = String.format("file://%s", new Object[] { System.getProperty("java.io.tmpdir") });
    private static final String UTF_8 = "UTF-8";

    public static void main(String[] args)
            throws Exception
    {
        if (args.length != 3) {
            System.out.println("Specify correct arguments");
        }
        String entitytype = args[0].trim().toLowerCase();
        String oldEntities = args[1];
        String outpath = args[2];
        changeEntities(entitytype, oldEntities, outpath);
    }

    public static void changeEntities(String entityType, String oldPath, String newPath)
            throws Exception
    {
        File folder = new File(oldPath);
        File[] listOfFiles = folder.listFiles();
        String stagePath = TMP_BASE_DIR + File.separator + newPath + File.separator + System.currentTimeMillis() / 1000L;
        System.out.println("Number of files: " + listOfFiles.length);
        for (File file : listOfFiles) {
            if (file.isFile())
            {
                System.out.println(file.getName());
                EntityType type = EntityType.getEnum(entityType);
                EntityParser<?> entityParser = EntityParserFactory.getParser(type);
                try
                {
                    InputStream xmlStream = new FileInputStream(file);
                    org.apache.falcon.entity.v0.process.Validity pek1Validity;
                    OutputStream out;
                    switch (type)
                    {
                        case PROCESS:
                            Process process = (Process)entityParser.parse(xmlStream);

                            org.apache.falcon.entity.v0.process.Clusters entityClusters = process.getClusters();
                            List<org.apache.falcon.entity.v0.process.Cluster> clusters = entityClusters.getClusters();
                            for (org.apache.falcon.entity.v0.process.Cluster cluster : clusters) {
                                if (cluster.getName().equals("hkg1-opal"))
                                {
                                    org.apache.falcon.entity.v0.process.Cluster pek1_cluster = new org.apache.falcon.entity.v0.process.Cluster();
                                    pek1_cluster.setName("pek1-pyrite");
                                    if (cluster.getSla() != null) {
                                        pek1_cluster.setSla(cluster.getSla());
                                    }
                                    pek1Validity = new org.apache.falcon.entity.v0.process.Validity();
                                    Date currentDate = new Date();
                                    currentDate.setTime(1488326400000L);
                                    pek1Validity.setStart(currentDate);
                                    pek1Validity.setEnd(cluster.getValidity().getEnd());
                                    pek1_cluster.setValidity(pek1Validity);
                                    clusters.add(pek1_cluster);
                                    break;
                                }
                            }
                            File entityFile = new File(new Path(newPath + File.separator + file.getName()).toUri().toURL().getPath());
                            System.out.println("File path : " + entityFile.getAbsolutePath());
                            if (!entityFile.createNewFile())
                            {
                                System.out.println("Not able to stage the entities in the tmp path");
                                return;
                            }
                            System.out.println(process.toString());
                            out = new FileOutputStream(entityFile);
                            type.getMarshaller().marshal(process, out);
                            out.close();
                            break;
                        case FEED:
                            Feed feed = (Feed)entityParser.parse(xmlStream);

                            org.apache.falcon.entity.v0.feed.Clusters feedClusters = feed.getClusters();
                            List<org.apache.falcon.entity.v0.feed.Cluster> feed_clusters = feedClusters.getClusters();
                            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed_clusters) {
                                if (cluster.getName().equals("hkg1-opal"))
                                {
                                    org.apache.falcon.entity.v0.feed.Cluster pek1_feed_cluster = new org.apache.falcon.entity.v0.feed.Cluster();
                                    pek1_feed_cluster.setName("pek1-pyrite");
                                    pek1_feed_cluster.setSla(cluster.getSla());
                                    pek1_feed_cluster.setDelay(cluster.getDelay());
                                    pek1_feed_cluster.setLifecycle(cluster.getLifecycle());
                                    pek1_feed_cluster.setLocations(cluster.getLocations());
                                    pek1_feed_cluster.setPartition(cluster.getPartition());
                                    pek1_feed_cluster.setRetention(cluster.getRetention());
                                    pek1_feed_cluster.setType(cluster.getType());

                                    org.apache.falcon.entity.v0.feed.Validity feedValidity = new org.apache.falcon.entity.v0.feed.Validity();
                                    Date currentDate = new Date();
                                    currentDate.setTime(1488326400000L);
                                    feedValidity.setStart(currentDate);
                                    feedValidity.setEnd(cluster.getValidity().getEnd());
                                    pek1_feed_cluster.setValidity(feedValidity);
                                    feed_clusters.add(pek1_feed_cluster);
                                    break;
                                }
                            }
                            entityFile = new File(new Path(newPath + File.separator + file.getName()).toUri().toURL().getPath());
                            System.out.println("File path : " + entityFile.getAbsolutePath());
                            if (!entityFile.createNewFile())
                            {
                                System.out.println("Not able to stage the entities in the tmp path");
                                return;
                            }
                            System.out.println(feed.toString());
                            out = new FileOutputStream(entityFile);
                            type.getMarshaller().marshal(feed, out);
                            out.close();
                    }
                }
                catch (FileNotFoundException e)
                {
                    System.out.println(e.toString());
                }
                catch (FalconException e)
                {
                    System.out.println(e.toString());
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
                catch (JAXBException e)
                {
                    System.out.println(e.toString());
                }
                catch (Exception e)
                {
                    System.out.println(e.toString());
                }
            }
        }
    }
}
