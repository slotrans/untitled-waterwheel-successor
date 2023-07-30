package net.blergh

import net.blergh.task.BuildMode
import net.blergh.task.SourceTask
import net.blergh.task.SqlTask
import net.blergh.task.Task
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.DirectedAcyclicGraph;
import spock.lang.Specification;

class AppTest extends Specification
{
    def "inferTaskStatus: a single SourceTask"() {
        given:
        def myTask = new SourceTask(RelName.fromString("foo.bar"))
        def myDAG = new DirectedAcyclicGraph<Task, DefaultEdge>(DefaultEdge.class)
        myDAG.addVertex(myTask)

        when:
        def inferredStatus = App.inferTaskStatus(myTask, myDAG)

        then:
        inferredStatus == TaskStatus.COMPLETE
    }

    def "inferTaskStatus: a single SqlTask"() {
        given:
        def myTask = new SqlTask(RelName.fromString("foo.bar"), "", "", BuildMode.FULL, null)
        def myDAG = new DirectedAcyclicGraph<Task, DefaultEdge>(DefaultEdge.class)
        myDAG.addVertex(myTask)

        when:
        def inferredStatus = App.inferTaskStatus(myTask, myDAG)

        then:
        inferredStatus == TaskStatus.READY
    }

    def "inferTaskStatus: one SourceTask, one SqlTask"() {
        given:
        def mySource = new SourceTask(RelName.fromString("foo.source_A"))
        def myTransform = new SqlTask(RelName.fromString("foo.transform_B"), "", "", BuildMode.FULL, null)
        def myDAG = new DirectedAcyclicGraph<Task, DefaultEdge>(DefaultEdge.class)
        myDAG.addVertex(mySource)
        myDAG.addVertex(myTransform)
        myDAG.addEdge(mySource, myTransform)

        when:
        def sourceStatus = App.inferTaskStatus(mySource, myDAG)
        def transformStatus = App.inferTaskStatus(myTransform, myDAG)

        then:
        sourceStatus == TaskStatus.COMPLETE
        transformStatus == TaskStatus.READY
    }

    def "inferTaskStatus: sourceA -> sqlB -> sqlC"() {
        given:
        def sourceA = new SourceTask(RelName.fromString("foo.source_A"))
        def transformB = new SqlTask(RelName.fromString("foo.transform_B"), "", "", BuildMode.FULL, null)
        def transformC = new SqlTask(RelName.fromString("foo.transform_C"), "", "", BuildMode.FULL, null)
        def myDAG = new DirectedAcyclicGraph<Task, DefaultEdge>(DefaultEdge.class)
        myDAG.addVertex(sourceA)
        myDAG.addVertex(transformB)
        myDAG.addVertex(transformC)
        myDAG.addEdge(sourceA, transformB)
        myDAG.addEdge(transformB, transformC)

        when:
        def sourceAStatus = App.inferTaskStatus(sourceA, myDAG)
        def transformBStatus = App.inferTaskStatus(transformB, myDAG)
        def transformCStatus = App.inferTaskStatus(transformC, myDAG)

        then:
        sourceAStatus == TaskStatus.COMPLETE
        transformBStatus == TaskStatus.READY
        transformCStatus == TaskStatus.WAITING
    }

    def "inferTaskStatus: sourceA -> sqlB -> sqlC where B is RUNNING"() {
        given:
        def sourceA = new SourceTask(RelName.fromString("foo.source_A"))
        def transformB = new SqlTask(RelName.fromString("foo.transform_B"), "", "", BuildMode.FULL, null)
        def transformC = new SqlTask(RelName.fromString("foo.transform_C"), "", "", BuildMode.FULL, null)
        def myDAG = new DirectedAcyclicGraph<Task, DefaultEdge>(DefaultEdge.class)
        myDAG.addVertex(sourceA)
        myDAG.addVertex(transformB)
        myDAG.addVertex(transformC)
        myDAG.addEdge(sourceA, transformB)
        myDAG.addEdge(transformB, transformC)
        transformB.knownTaskStatus = Optional.of(TaskStatus.RUNNING) //icky, breaks invariants, but ok for this test

        when:
        def sourceAStatus = App.inferTaskStatus(sourceA, myDAG)
        def transformBStatus = App.inferTaskStatus(transformB, myDAG)
        def transformCStatus = App.inferTaskStatus(transformC, myDAG)

        then:
        sourceAStatus == TaskStatus.COMPLETE
        transformBStatus == TaskStatus.RUNNING
        transformCStatus == TaskStatus.WAITING
    }

    def "inferTaskStatus: sourceA -> sqlB -> sqlC where B is FAILED"() {
        given:
        def sourceA = new SourceTask(RelName.fromString("foo.source_A"))
        def transformB = new SqlTask(RelName.fromString("foo.transform_B"), "", "", BuildMode.FULL, null)
        def transformC = new SqlTask(RelName.fromString("foo.transform_C"), "", "", BuildMode.FULL, null)
        def myDAG = new DirectedAcyclicGraph<Task, DefaultEdge>(DefaultEdge.class)
        myDAG.addVertex(sourceA)
        myDAG.addVertex(transformB)
        myDAG.addVertex(transformC)
        myDAG.addEdge(sourceA, transformB)
        myDAG.addEdge(transformB, transformC)
        transformB.knownTaskStatus = Optional.of(TaskStatus.FAILED) //similarly icky

        when:
        def sourceAStatus = App.inferTaskStatus(sourceA, myDAG)
        def transformBStatus = App.inferTaskStatus(transformB, myDAG)
        def transformCStatus = App.inferTaskStatus(transformC, myDAG)

        then:
        sourceAStatus == TaskStatus.COMPLETE
        transformBStatus == TaskStatus.FAILED
        transformCStatus == TaskStatus.UPSTREAM_FAILED
    }

}