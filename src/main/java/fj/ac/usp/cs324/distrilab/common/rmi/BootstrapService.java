package fj.ac.usp.cs324.distrilab.common.rmi;

import fj.ac.usp.cs324.distrilab.common.model.WorkerInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface BootstrapService extends Remote {

    void registerWorker(WorkerInfo worker) throws RemoteException;

    void unregisterWorker(int workerId) throws RemoteException;

    List<WorkerInfo> getActiveWorkers() throws RemoteException;

    WorkerInfo getRandomWorker() throws RemoteException;
}