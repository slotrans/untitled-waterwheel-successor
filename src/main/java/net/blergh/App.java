package net.blergh;

import net.blergh.task.BuildMode;
import net.blergh.task.SourceTask;
import net.blergh.task.SqlTask;
import net.blergh.task.Task;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.*;
import org.jgrapht.graph.builder.GraphBuilder;
import org.jgrapht.nio.dot.DOTImporter;
import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class App
{
    private static final Logger log = LoggerFactory.make();
    public static void main( String[] args ) throws Exception
    {
        ArgumentParser parser = ArgumentParsers.newFor("untitled-waterwheel-successor").build().description("BUILD SOME TABLES WOOOOO");
        parser.addArgument("-j", "--journal")
                .type(String.class)
                .required(false)
                .help("journal file from which to load previous execution state (NOT YET IMPLEMENTED");
        parser.addArgument("-s", "--selector")
                .type(String.class)
                .required(false)
                .help("target selection expression (NOT YET IMPLEMENTED)");

        try
        {
            Namespace parsedArgs = parser.parseArgs(args);
            realMain();
        }
        catch (ArgumentParserException e)
        {
            parser.handleError(e);
        }
    }

    private static void realMain() throws IOException //TODO: figure out where IOException should be handled
    {
        //we'll use this DAG to accumulate dependencies as we load each task
        final DirectedAcyclicGraph<RelName, DefaultEdge> nameDAG = new DirectedAcyclicGraph<>(DefaultEdge.class);

        //seed the dependency graph with declared sources
        //TODO: import source_tables.gv into the above graph

        //keep track of all tasks by their name
        final Map<RelName, Task> taskTable = new HashMap<>();

        nameDAG.forEach( nodeRelName -> {
            SourceTask sourceTask = new SourceTask(nodeRelName);
            taskTable.put(nodeRelName, sourceTask);
        });

        //find all src/<schema>/<table>/ directories
        log.debug("enumerating script directories...");
        final List<Path> srcDirectoryPaths = Files.find(
                    Path.of("src"),
                    3,
                    (path, basicFileAttributes) -> basicFileAttributes.isDirectory(),
                    FileVisitOption.FOLLOW_LINKS
        ).toList();

        //try to load a task from each directory
        for( Path tableDirectoryPath : srcDirectoryPaths )
        {
            log.debug("entering directory {}", tableDirectoryPath);
            if( tableDirectoryPath.getNameCount() != 3)
            {
                log.debug("ignoring directory {}", tableDirectoryPath);
                continue;
            }

            boolean hasCreate = false;
            boolean hasBuild = false;
            boolean hasDeps = false;
            String createScript = "";
            String buildScript = "";
            String depsFragment = "";
            Properties config = new Properties();
            config.put("buildMode", BuildMode.FULL.toString());

            for(Path filePath : Files.list(tableDirectoryPath).toList() )
            {
                if( "create.sql".equals(filePath.getFileName().toString()) )
                {
                    hasCreate = true;
                    createScript = Files.readString(filePath);
                }

                if( "build.sql".equals(filePath.getFileName().toString()) )
                {
                    hasBuild = true;
                    buildScript = Files.readString(filePath);
                }

                if( "deps.gv".equals(filePath.getFileName().toString()) )
                {
                    hasDeps = true;
                    depsFragment = Files.readString(filePath);
                }

                if( "config.properties".equals(filePath.getFileName().toString()) )
                {
                    Properties fileProps = new Properties();
                    fileProps.load(Files.newBufferedReader(filePath));
                    config.putAll(fileProps);
                }
            }

            final String schemaName = tableDirectoryPath.getName(1).toString();
            final String tableName = tableDirectoryPath.getName(2).toString();
            final RelName relName = new RelName(schemaName, tableName);

            //TODO: check if each task has a sensible combination of files, and warn (error?) if not
            //TODO: perhaps a useful CLI flag would be to fail-fast if a task appears to be invalid

            log.info("constructing SqlTask for {}", relName.toFullName());
            SqlTask sqlTask = new SqlTask(
                    relName,
                    createScript,
                    buildScript,
                    BuildMode.valueOf((String) config.get("buildMode")),
                    null //TODO
            );
            taskTable.put(relName, sqlTask);
        }

        //now that we've loaded all the tasks, transform the DAG of names into a DAG of tasks
        final DirectedAcyclicGraph<Task, DefaultEdge> taskDAG = new DirectedAcyclicGraph<>(DefaultEdge.class);
        //might be able to eliminate this loop by using a VertexSupplier
        for( RelName nodeRelName : nameDAG.vertexSet() )
        {
            taskDAG.addVertex(taskTable.get(nodeRelName));
        }
        for( DefaultEdge edge : nameDAG.edgeSet() )
        {
            Task source = taskTable.get(nameDAG.getEdgeSource(edge));
            Task target = taskTable.get(nameDAG.getEdgeTarget(edge));
            taskDAG.addEdge(source, target);
        }

        //the driving loop will use a List of tasks in Topological Sort order (which DAG's forEach produces)
        List<Task> workingSet = new ArrayList<>();
        taskDAG.forEach(workingSet::add); //DAGs iterate in Topological Sort order

        //driving loop:
//        while(!workingSet.isEmpty())
//        {
//
//        }
    }

    private static void importGraphFromFile()
    {
        Graph<String, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        DOTImporter<String, DefaultEdge> importer = new DOTImporter<>();
        importer.setVertexFactory(x -> x); // read the vertex name as a string, return it unmodified

        System.out.println("importing graph from complete_deps.gv");
        try (Reader reader = new FileReader("complete_deps.gv"))
        {
            importer.importGraph(graph, reader);

            System.out.println("edges:");
            for(DefaultEdge edge : graph.edgeSet())
            {
                System.out.println(edge.toString());
            }
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void walkSrcDir() throws IOException
    {
        final List<Path> srcPaths = Files.find(
                Path.of("src"),
                3,
                (path, basicFileAttributes) -> basicFileAttributes.isDirectory(),
                FileVisitOption.FOLLOW_LINKS
        ).toList();

        for( Path tableDirectoryPath : srcPaths )
        {
            // the only split length we're interested in is 3, e.g. src/schemaname/tablename -> ['src', 'schemaname', 'tablename']
            // src and src/schemaname (split < 3)are just ignored
            // src/schemaname/tablename/junk (split > 3) is permitted, if users want to store notes, scratch code, design docs, whatever
            if( tableDirectoryPath.getNameCount() != 3)
            {
                log.debug("ignoring directory {}", tableDirectoryPath);
                continue;
            }

            final String schemaName = tableDirectoryPath.getName(1).toString();
            final String tableName = tableDirectoryPath.getName(2).toString();
            final RelName relName = new RelName(schemaName, tableName);
            //TODO: equivalent of BUILD_SCOPE.add(node_name)

            final List<Path> filesInDir = Files.list(tableDirectoryPath)
                    .map(Path::getFileName)
                    .toList();
            final boolean hasCreate = filesInDir.contains(Path.of("create.sql"));
            final boolean hasBuild = filesInDir.contains(Path.of("build.sql"));
            final boolean hasDeps = filesInDir.contains(Path.of("deps.gv"));

            if( hasCreate && hasBuild && hasDeps )
            {
                // this is what we want
                log.debug("{} has all necessary scripts", tableDirectoryPath);
            }
            else if( !hasCreate && !hasBuild && !hasDeps )
            {
                log.warn("{} has no scripts, ignoring", tableDirectoryPath);
                //TODO: equivalent of BUILD_SCOPE.remove(node_name)
            }
            else if( hasCreate && hasBuild && !hasDeps )
            {
                // is this right? i don't think it is...
                // if deps.gv exists but is empty, that would logically be equivalent, but would not fall into this branch
                // also if another table declares a dependency on this table, that would put it in the graph
                log.warn("{} missing deps.gv, table may only be built directly", tableDirectoryPath);

                //2023-07-02:
                //  I don't think this IS right, in the new design.
                //  In general it should be ok to have a table with no dependencies, even if that's weird.
                //  We may need to take additional steps to ensure such tables end up in the graph.
            }
            else
            {
                log.error("{} missing sufficient scripts, any build involving it will fail", tableDirectoryPath);
            }
        }
    }
}
