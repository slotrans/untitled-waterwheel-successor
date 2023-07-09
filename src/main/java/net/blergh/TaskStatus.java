package net.blergh;

public enum TaskStatus
{
    INITIAL(false),
    WAITING(false),
    READY(false),
    RUNNING(false),
    COMPLETE(true),
    FAILED(true),
    UPSTREAM_FAILED(true);

    private final boolean isTerminal;

    public boolean isTerminal() { return isTerminal; }

    TaskStatus(boolean isTerminal)
    {
        this.isTerminal = isTerminal;
    }
}
