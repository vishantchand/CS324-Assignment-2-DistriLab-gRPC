# CS324-Assignment-1-DistriLab
CS324 Distributed Systems Assignment 1 - Java RMI distributed computing cluster

DistriLab Distributed Computing Cluster

DistriLab is a Java 21 distributed-computing system developed for CS324 Assignment 1. It uses Java RMI to connect a Bootstrap Node, an unstructured network of Worker Nodes, and multiple Swing GUI clients.

The system elects a coordinator, divides jobs among reachable workers, combines partial results, monitors failed workers, and elects a new coordinator after a five-job term.

Features

- Bootstrap Node for worker registration and discovery
- Automatic removal of unreachable workers
- Unstructured worker network with bidirectional neighbour connections
- Custom leader-election algorithm
- Duplicate election-message prevention
- Lowest Job Allocation Counter (JAC) used to select the coordinator
- Highest worker ID used as the tie-breaker
- Automatic leader election after five coordinator jobs
- Distributed workload partitioning and result aggregation
- Worker-failure recovery
- Concurrent job processing using Java threads
- Swing GUI supporting multiple clients
- Manual and CSV input
- MAX, PRIMECOUNT and PRIMESUM jobs

Requirements

- Java Development Kit 21
- Maven 3.9 or later, or IntelliJ IDEA with bundled Maven
- Git
- Windows, Linux or macOS
- Computers on the same reachable network for multi-computer testing

Verify Java:

    java -version
    javac -version

Both commands should report Java 21.

Project Structure

    src/
    |-- main/java/fj/ac/usp/cs324/distrilab/
    |   |-- bootstrap/    Bootstrap Node
    |   |-- client/       Swing GUI client
    |   |-- common/       Shared models and RMI interfaces
    |   |-- election/     Leader-election manager
    |   `-- worker/       Worker service and job engine
    `-- test/java/fj/ac/usp/cs324/distrilab/
        |-- bootstrap/    Bootstrap tests
        |-- election/     Election tests
        `-- worker/       Worker and integration tests

Download and Build

Clone the repository:

    git clone https://github.com/vishantchand/CS324-Assignment-1-DistriLab.git
    cd CS324-Assignment-1-DistriLab
    git switch main
    git pull origin main

Compile and run the tests:

    mvn clean verify

Expected result:

    Tests run: 42, Failures: 0, Errors: 0, Skipped: 0
    BUILD SUCCESS

If `mvn` is not recognised, use IntelliJ IDEA's Maven panel and run `Lifecycle > clean` followed by `Lifecycle > verify`.

Running on One Computer

Use a separate terminal for the Bootstrap Node, each Worker Node and each client.

 1. Start the Bootstrap Node

    java -cp target\classes fj.ac.usp.cs324.distrilab.bootstrap.BootstrapServer 1099

Expected output:

    Bootstrap Node started successfully.
    Registry port: 1099
    Binding name: BootstrapService
    Worker health-check interval: 5 seconds
    Waiting for worker registrations...

 2. Start Three Worker Nodes

Worker 1:

    java -cp target\classes fj.ac.usp.cs324.distrilab.worker.WorkerServer 1 localhost 1101 localhost 1099

Worker 2:

    java -cp target\classes fj.ac.usp.cs324.distrilab.worker.WorkerServer 2 localhost 1102 localhost 1099

Worker 3:

    java -cp target\classes fj.ac.usp.cs324.distrilab.worker.WorkerServer 3 localhost 1103 localhost 1099

Worker command format:

    WorkerServer <worker-id> <worker-host> <worker-port> <bootstrap-host> <bootstrap-port>

Every worker must have a unique worker ID and registry port.

 3. Start Two Clients

Run the following command in two separate terminals:

    java -cp target\classes fj.ac.usp.cs324.distrilab.client.DistriLabClient

Enter these details in both clients:

    Bootstrap host: localhost
    Bootstrap port: 1099

 Running Across Two Computers

The completed integration test used:

- Bootstrap computer: `10.31.242.247`
- Worker computer: `10.31.242.198`

Replace these addresses if the computers receive different IP addresses.

 1. Start Bootstrap on the Bootstrap Computer

In Windows PowerShell:

    java "-Djava.rmi.server.hostname=10.31.242.247" -cp target\classes fj.ac.usp.cs324.distrilab.bootstrap.BootstrapServer 1099

 2. Start Workers on the Worker Computer

Worker 1:

    java -cp target\classes fj.ac.usp.cs324.distrilab.worker.WorkerServer 1 10.31.242.198 1101 10.31.242.247 1099

Worker 2:

    java -cp target\classes fj.ac.usp.cs324.distrilab.worker.WorkerServer 2 10.31.242.198 1102 10.31.242.247 1099

Worker 3:

    java -cp target\classes fj.ac.usp.cs324.distrilab.worker.WorkerServer 3 10.31.242.198 1103 10.31.242.247 1099

The Bootstrap terminal should display registrations for Workers 1, 2 and 3.

 3. Start the Clients

    java -cp target\classes fj.ac.usp.cs324.distrilab.client.DistriLabClient

Enter:

    Bootstrap host: 10.31.242.247
    Bootstrap port: 1099

For multi-computer testing, allow Java through Windows Defender Firewall on private networks. Ports 1099, 1101, 1102 and 1103 must be reachable.

 Submitting Jobs

 MAX

Select `MAX` and enter an unsorted number list.

Example:

    7, 20, 4, 35, 9, 50, 1, 22, 18, 6

Expected result:

    50

 PRIMECOUNT

Select `PRIMECOUNT` and enter an unsorted number list.

Example:

    2, 3, 4, 5, 11, 12, 13, 17, 20, 23

Expected result:

    7

 PRIMESUM

Select `PRIMESUM` and enter the range.

Example:

    Range start: 1
    Range end: 100

Expected result:

    1060

The results table displays the job ID, job type, status, result and completion message. Widen the Message column to see how many worker partitions were used.

 CSV Input

Select the required job type before clicking `Load CSV`.

For MAX and PRIMECOUNT, the CSV file must contain whole numbers separated by commas, semicolons, spaces or new lines. Do not include a heading.

Example:

    2,3,4,5,11,12,13,17,20,23

For PRIMESUM, the CSV file must contain exactly two whole numbers: the range start and range end.

Example:

    1,100

 Concurrent Client Test

1. Keep the Bootstrap Node and all three workers running.
2. Launch two GUI client processes.
3. Submit jobs from both clients.
4. Confirm that both result tables show `Success`.
5. Confirm that the returned results are correct.

 Leader Re-election Test

1. Start the Bootstrap Node and three workers.
2. Submit five successful jobs.
3. Check the Worker terminals.
4. Confirm that the coordinator reports that five jobs were completed.
5. Confirm that a new election begins.
6. Confirm that all reachable workers accept the same coordinator and election ID.

 Worker-Failure Test

1. Start the Bootstrap Node, three workers and two clients.
2. Submit a distributed job and confirm that it succeeds.
3. Force-stop one non-coordinator worker.
4. Wait at least five seconds.
5. Confirm that the Bootstrap Node removes the unreachable worker.
6. Submit another job.
7. Confirm that it succeeds using the remaining workers.

To force-stop Worker 3 on Windows:

    $workerProcessId = Get-NetTCPConnection -LocalPort 1103 -State Listen | Select-Object -First 1 -ExpandProperty OwningProcess
    Stop-Process -Id $workerProcessId -Force

 Stopping the System

Press `Ctrl+C` once in every terminal running a Worker Node or Bootstrap Node.

A normally stopped worker attempts to notify its neighbours and unregister itself from the Bootstrap Node.

 Troubleshooting

 Maven Is Not Recognised

Use IntelliJ IDEA's Maven panel, or add Maven's `bin` directory to the Windows PATH.

 Client Class Cannot Be Found

Update and rebuild the project:

    git switch main
    git pull origin main
    mvn clean verify

Confirm that `target\classes` exists and that the command is running from the repository root.

 Port Is Already in Use

Find the process using port 1099:

    Get-NetTCPConnection -LocalPort 1099 -State Listen | Select-Object OwningProcess

After confirming it belongs to an old DistriLab process, stop it:

    Stop-Process -Id <process-id> -Force

 PowerShell Treats Output as Commands

Enter only the command itself. Do not paste the `PS C:\...>` prompt or output produced by a previous command.

 Remote Workers Do Not Register

- Confirm both computers are connected to the same network.
- Use `ipconfig` to check their current IPv4 addresses.
- Test connectivity using `ping`.
- Do not use `localhost` for cross-computer communication.
- Allow Java through the private-network firewall.
- Confirm ports 1099 and 1101–1103 are available.

 Contributors and Responsibilities

- Vishant Chand — shared RMI contracts, Bootstrap Node, Bootstrap health monitoring, client GUI, integration coordination and testing
- Shanesh Dewan — Worker Node startup, neighbour management, worker health monitoring and failure testing
- Ammara — job engine, distributed partitioning, aggregation and job-failure recovery
- Hritish — leader election, coordinator propagation, duplicate-message handling and election integration testing

 Verified Results

The final integration test used:

- One Bootstrap Node
- Three Worker Nodes
- Two simultaneous GUI clients
- Two computers connected over a local network

MAX, PRIMECOUNT and PRIMESUM returned correct results. Distributed partitioning was confirmed, a new coordinator was elected after five jobs, and job processing continued after one worker was force-stopped.

Final automated verification:

    Tests run: 42
    Failures: 0
    Errors: 0
    Skipped: 0
    BUILD SUCCESS
