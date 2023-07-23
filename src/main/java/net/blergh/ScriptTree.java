package net.blergh;

import net.blergh.task.BuildMode;
import net.blergh.task.SourceTask;
import net.blergh.task.SqlTask;
import net.blergh.task.Task;
import org.jdbi.v3.core.Jdbi;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.nio.dot.DOTImporter;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/*
I'm not SURE if this is a good way / the right way to encapsulate walking the script tree and parsing tasks,
but it's something.
 */
public class ScriptTree
{
    private static final Logger log = LoggerFactory.make();

    private static final String SOURCE_RELATIONS_DOT_GV = "source_relations.gv";
    private static final String CREATE_DOT_SQL = "create.sql";
    private static final String BUILD_DOT_SQL = "build.sql";
    private static final String DEPS_DOT_GV = "deps.gv";
    private static final String CONFIG_DOT_PROPERTIES = "build.properties";
    private static final String BUILD_MODE_PROP = "buildMode";


    final Jdbi jdbi;
    final Path basePath;

    final DirectedAcyclicGraph<RelName, DefaultEdge> nameDAG;
    final DirectedAcyclicGraph<RelName, DefaultEdge> sourceRelations;

    final Map<RelName, Task> taskTable;
    final DirectedAcyclicGraph<Task, DefaultEdge> taskDAG;


    public ScriptTree(String baseDirectoryName, Jdbi jdbi) throws IOException //TODO: arrrrrgh
    {
        this.jdbi = jdbi;
        this.basePath = Path.of(baseDirectoryName);

        this.nameDAG = new DirectedAcyclicGraph<>(DefaultEdge.class);
        this.sourceRelations = readNameDAGFragment(basePath.resolve(SOURCE_RELATIONS_DOT_GV));

        this.taskTable = new HashMap<>();
        this.taskDAG = new DirectedAcyclicGraph<>(DefaultEdge.class);

        build();
    }

    private void build() throws IOException
    {
        //seed the dependency graph with declared sources
        Graphs.addAllVertices(nameDAG, sourceRelations.vertexSet());
        //and matching tasks
        nameDAG.forEach( nodeRelName -> {
            log.info("constructing SourceTask for {}", nodeRelName.toFullName());
            SourceTask sourceTask = new SourceTask(nodeRelName);
            taskTable.put(nodeRelName, sourceTask);
        });


        //find all src/<schema>/<table>/ directories
        log.debug("enumerating script directories...");
        final List<Path> srcDirectoryPaths = Files.find(
                basePath,
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
            config.put(BUILD_MODE_PROP, BuildMode.FULL.toString()); //default build mode is FULL

            DirectedAcyclicGraph<RelName, DefaultEdge> oneTaskDAG;

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
                    jdbi
            );
            taskTable.put(relName, sqlTask);
            if( depsFragment != null && !depsFragment.isEmpty() )
            {
                oneTaskDAG = buildNameDAGFragment(depsFragment);
                //TODO: one of these NPEs if a task depends on a non-existent source
                Graphs.addAllVertices(nameDAG, oneTaskDAG.vertexSet());
                Graphs.addAllEdges(nameDAG, oneTaskDAG, oneTaskDAG.edgeSet());
            }
        }

        //now that we've loaded all the tasks, transform the DAG of names into a DAG of tasks
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
    }

    private static DirectedAcyclicGraph<RelName, DefaultEdge> buildNameDAGFragment(String dotString)
    {
        final DirectedAcyclicGraph<RelName, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
        final DOTImporter<RelName, DefaultEdge> importer = new DOTImporter<>();
        importer.setVertexFactory(RelName::fromString);

        final String wrapped = "digraph {\n" + dotString + "\n}";

        importer.importGraph(graph, new StringReader(wrapped));
        return graph;
    }

    private static DirectedAcyclicGraph<RelName, DefaultEdge> readNameDAGFragment(Path path) throws IOException
    {
        final String fileContents = Files.readString(path);

        return buildNameDAGFragment(fileContents);
    }


    public DirectedAcyclicGraph<Task, DefaultEdge> getTaskDAG()
    {
        return taskDAG;
    }

    public Map<RelName, Task> getTaskTable()
    {
        return taskTable;
    }
}
