package fj.ac.usp.cs324.distrilab.common.rmi;

import fj.ac.usp.cs324.distrilab.common.model.CoordinatorMessage;
import fj.ac.usp.cs324.distrilab.common.model.ElectionMessage;
import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface WorkerService extends Remote {

    WorkerInfo getWorkerInfo() throws RemoteException;

    boolean ping() throws RemoteException;

    void addNeighbor(WorkerInfo worker) throws RemoteException;

    void removeNeighbor(int workerId) throws RemoteException;

    List<WorkerInfo> getNeighbors() throws RemoteException;

    void startElection() throws RemoteException;

    ElectionMessage receiveElection(ElectionMessage message)
            throws RemoteException;

    void receiveCoordinator(CoordinatorMessage message)
            throws RemoteException;

    JobResult submitJob(JobRequest request) throws RemoteException;

    JobResult executeJob(JobRequest request) throws RemoteException;
}