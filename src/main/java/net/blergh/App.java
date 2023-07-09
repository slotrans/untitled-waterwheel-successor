package net.blergh;

import net.blergh.task.BuildMode;
import net.blergh.task.SourceTask;
import net.blergh.task.SqlTask;
import net.blergh.task.Task;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jgrapht.Graphs;
import org.jgrapht.graph.*;
import org.jgrapht.nio.dot.DOTImporter;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class App
{
    private static final Logger log = LoggerFactory.make();

    private static final String SRC = "src";
    private static final String SOURCE_RELATIONS_DOT_GV = SRC+"/source_relations.gv";
    private static final String CREATE_DOT_SQL = "create.sql";
    private static final String BUILD_DOT_SQL = "build.sql";
    private static final String DEPS_DOT_GV = "deps.gv";
    private static final String CONFIG_DOT_PROPERTIES = "build.properties";
    private static final String BUILD_MODE_PROP = "buildMode";

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
        final DirectedAcyclicGraph<RelName, DefaultEdge> sourceRelations = importNameDAGFragment(Path.of(SOURCE_RELATIONS_DOT_GV));
        Graphs.addAllVertices(nameDAG, sourceRelations.vertexSet());

        //keep track of all tasks by their name
        final Map<RelName, Task> taskTable = new HashMap<>();

        nameDAG.forEach( nodeRelName -> {
            log.info("constructing SourceTask for {}", nodeRelName.toFullName());
            SourceTask sourceTask = new SourceTask(nodeRelName);
            taskTable.put(nodeRelName, sourceTask);
        });

        //find all src/<schema>/<table>/ directories
        log.debug("enumerating script directories...");
        final List<Path> srcDirectoryPaths = Files.find(
                    Path.of(SRC),
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
            config.put(BUILD_MODE_PROP, BuildMode.FULL.toString());

            for(Path filePath : Files.list(tableDirectoryPath).toList() )
            {
                if( CREATE_DOT_SQL.equals(filePath.getFileName().toString()) )
                {
                    hasCreate = true;
                    createScript = Files.readString(filePath);
                }

                if( BUILD_DOT_SQL.equals(filePath.getFileName().toString()) )
                {
                    hasBuild = true;
                    buildScript = Files.readString(filePath);
                }

                if( DEPS_DOT_GV.equals(filePath.getFileName().toString()) )
                {
                    hasDeps = true;
                    depsFragment = Files.readString(filePath);
                }

                if( CONFIG_DOT_PROPERTIES.equals(filePath.getFileName().toString()) )
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
                    BuildMode.valueOf((String) config.get(BUILD_MODE_PROP)),
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

    private static DirectedAcyclicGraph<RelName, DefaultEdge> importNameDAGFragment(Path path) throws IOException
    {
        final DirectedAcyclicGraph<RelName, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
        final DOTImporter<RelName, DefaultEdge> importer = new DOTImporter<>();
        importer.setVertexFactory(RelName::fromString);

        final String fileContents = Files.readString(path);
        final String wrapped = "digraph {\n" + fileContents + "\n}";

        importer.importGraph(graph, new StringReader(wrapped));
        return graph;
    }

}
