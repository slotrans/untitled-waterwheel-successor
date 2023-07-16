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

    //TODO: take this as a parameter?
    private static final String SRC = "src";

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
        parser.addArgument("-t", "--threads")
                .type(Integer.class)
                .required(false)
                .setDefault(1)
                .help("number of worker threads");

        try
        {
            Namespace parsedArgs = parser.parseArgs(args);
            int threads = parsedArgs.get("threads");
            realMain(threads);
        }
        catch (ArgumentParserException e)
        {
            parser.handleError(e);
        }
    }

    private static void realMain(final int maxThreads) throws IOException //TODO: figure out where IOException should be handled
    {
        ScriptTree scriptTree = new ScriptTree(SRC, null); //TODO: pass an actual Jdbi instance
        final DirectedAcyclicGraph<Task, DefaultEdge> taskDAG = scriptTree.getTaskDAG();
        final Map<RelName, Task> taskTable = scriptTree.getTaskTable();


        //the driving loop will use a List of tasks in Topological Sort order (which DAG's forEach produces)
        List<Task> workingSet = new ArrayList<>();
        taskDAG.forEach(workingSet::add); //DAGs iterate in Topological Sort order
        log.debug("initial workingSet:");
        workingSet.forEach(item -> log.debug("  {}: {}", item.getClass().getName(), item.getRelName().toFullName()));
        //TODO: export to a DOT file


        //driving loop
        int drivingLoopIterations = 0;
        int runningThreads = 0;
        while(!workingSet.isEmpty())
        {
            List<Task> nextWorkingSet = new ArrayList<>();
            for( Task task : workingSet )
            {
                TaskStatus status = inferTaskStatus(task, taskDAG);

            }

            workingSet = nextWorkingSet;
            drivingLoopIterations++;

            break; //TESTING!
        }
    }

    private static TaskStatus inferTaskStatus(Task taskInQuestion, DirectedAcyclicGraph<Task, DefaultEdge> graph)
    {
        //recursive base case
        final Optional<TaskStatus> knownTaskStatus = taskInQuestion.getKnownTaskStatus();
        if( knownTaskStatus.isPresent() )
        {
            return knownTaskStatus.get();
        }

        for( Task predecessorTask : Graphs.predecessorListOf(graph, taskInQuestion) )
        {
            TaskStatus predecessorStatus = inferTaskStatus(predecessorTask, graph);
            switch(predecessorStatus)
            {
                case FAILED:
                case UPSTREAM_FAILED:
                    taskInQuestion.upstreamFailureObserved();
                    return TaskStatus.UPSTREAM_FAILED;
                case WAITING:
                case READY:
                case RUNNING:
                    return TaskStatus.WAITING;
                case COMPLETE:
                    break; //no-op
            }
        }
        //if we didn't return already that means ALL predecessor tasks are COMPLETE
        return TaskStatus.READY;
    }

    private static void journalTaskState(Task task, TaskStatus status)
    {
        //TODO: real journaling
        System.out.println("JOURNAL: \"" + task.getRelName().toFullName() + "\" -> " + status);
    }
}
