package net.blergh.task;

import net.blergh.RelName;
import net.blergh.TaskStatus;

import java.util.Optional;

public interface Task //TODO: should this be sealed?
{
    RelName getRelName();

    Optional<TaskStatus> getKnownTaskStatus();

    void upstreamFailureObserved();

    boolean start();

    int getThreadWeight();

    //TODO: public Optional<String> getResult() or something
}
