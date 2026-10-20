package fj.ac.usp.cs324.distrilab.client;

import fj.ac.usp.cs324.distrilab.common.model.JobRequest;
import fj.ac.usp.cs324.distrilab.common.model.JobResult;
import fj.ac.usp.cs324.distrilab.common.model.JobType;
import fj.ac.usp.cs324.distrilab.grpc.JobProgress;
import fj.ac.usp.cs324.distrilab.grpc.WorkerProgress;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class DistriLabClient extends JFrame {

    private static final int PROGRESS_COLUMN = 3;

    private final JTextField bootstrapHostField =
            new JTextField("localhost", 12);

    private final JTextField bootstrapPortField =
            new JTextField("50051", 6);

    private final JComboBox<JobType> jobTypeComboBox =
            new JComboBox<>(JobType.values());

    private final JTextArea numbersArea =
            new JTextArea(4, 35);

    private final JTextField rangeStartField =
            new JTextField(10);

    private final JTextField rangeEndField =
            new JTextField(10);

    private final JButton loadCsvButton =
            new JButton("Load CSV");

    private final JButton submitButton =
            new JButton("Submit Job");

    private final JLabel statusLabel =
            new JLabel("Ready");

    private final DefaultTableModel resultTableModel =
            new DefaultTableModel(
                    new Object[]{
                            "Job ID",
                            "Type",
                            "Status",
                            "Progress",
                            "Result",
                            "Message"
                    },
                    0
            ) {
                @Override
                public boolean isCellEditable(
                        int row,
                        int column
                ) {
                    return false;
                }

                @Override
                public Class<?> getColumnClass(
                        int columnIndex
                ) {
                    if (columnIndex == PROGRESS_COLUMN) {
                        return Integer.class;
                    }

                    return Object.class;
                }
            };

    private final JTable resultTable =
            new JTable(resultTableModel);

    private final Map<String, JobProgress> latestJobProgress =
            new HashMap<>();

    public DistriLabClient() {
        super("DistriLab Client");

        configureWindow();
        configureActions();
        updateInputFields();
    }

    private void configureWindow() {
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1100, 650);
        setLocationRelativeTo(null);

        JPanel rootPanel = new JPanel(
                new BorderLayout(10, 10)
        );

        rootPanel.setBorder(
                BorderFactory.createEmptyBorder(
                        12,
                        12,
                        12,
                        12
                )
        );

        rootPanel.add(
                createInputPanel(),
                BorderLayout.NORTH
        );

        configureResultTable();

        rootPanel.add(
                new JScrollPane(resultTable),
                BorderLayout.CENTER
        );

        JPanel statusPanel =
                new JPanel(new FlowLayout(FlowLayout.LEFT));

        statusPanel.add(statusLabel);

        rootPanel.add(
                statusPanel,
                BorderLayout.SOUTH
        );

        setContentPane(rootPanel);
    }

    private void configureResultTable() {
        resultTable.setFillsViewportHeight(true);
        resultTable.setAutoCreateRowSorter(true);
        resultTable.setRowHeight(26);

        resultTable.getColumnModel()
                .getColumn(PROGRESS_COLUMN)
                .setCellRenderer(new ProgressBarRenderer());

        resultTable.getColumnModel()
                .getColumn(PROGRESS_COLUMN)
                .setPreferredWidth(130);

        resultTable.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent event) {
                int viewRow = resultTable.rowAtPoint(
                        event.getPoint()
                );

                int viewColumn = resultTable.columnAtPoint(
                        event.getPoint()
                );

                if (viewRow < 0 || viewColumn < 0) {
                    return;
                }

                int modelRow =
                        resultTable.convertRowIndexToModel(
                                viewRow
                        );

                int modelColumn =
                        resultTable.convertColumnIndexToModel(
                                viewColumn
                        );

                if (modelColumn == PROGRESS_COLUMN) {
                    showWorkerProgress(modelRow);
                }
            }
        });
    }

    private JPanel createInputPanel() {
        JPanel outerPanel =
                new JPanel(new BorderLayout(8, 8));

        outerPanel.setBorder(
                BorderFactory.createTitledBorder(
                        "Job Submission"
                )
        );

        JPanel formPanel =
                new JPanel(new GridBagLayout());

        GridBagConstraints constraints =
                new GridBagConstraints();

        constraints.insets = new Insets(4, 4, 4, 4);
        constraints.anchor =
                GridBagConstraints.WEST;

        addField(
                formPanel,
                constraints,
                0,
                "Bootstrap host:",
                bootstrapHostField
        );

        addField(
                formPanel,
                constraints,
                1,
                "Bootstrap port:",
                bootstrapPortField
        );

        addField(
                formPanel,
                constraints,
                2,
                "Job type:",
                jobTypeComboBox
        );

        constraints.gridx = 0;
        constraints.gridy = 3;
        constraints.anchor =
                GridBagConstraints.NORTHWEST;

        formPanel.add(
                new JLabel(
                        "Numbers for MAX/PRIMECOUNT:"
                ),
                constraints
        );

        numbersArea.setLineWrap(true);
        numbersArea.setWrapStyleWord(true);
        numbersArea.setToolTipText(
                "Enter numbers separated by commas, "
                        + "spaces or new lines"
        );

        constraints.gridx = 1;
        constraints.fill =
                GridBagConstraints.HORIZONTAL;
        constraints.weightx = 1.0;

        formPanel.add(
                new JScrollPane(numbersArea),
                constraints
        );

        constraints.fill =
                GridBagConstraints.NONE;
        constraints.weightx = 0.0;

        addField(
                formPanel,
                constraints,
                4,
                "Range start for PRIMESUM:",
                rangeStartField
        );

        addField(
                formPanel,
                constraints,
                5,
                "Range end for PRIMESUM:",
                rangeEndField
        );

        outerPanel.add(
                formPanel,
                BorderLayout.CENTER
        );

        JPanel buttonPanel =
                new JPanel(new FlowLayout(FlowLayout.RIGHT));

        buttonPanel.add(loadCsvButton);
        buttonPanel.add(submitButton);

        outerPanel.add(
                buttonPanel,
                BorderLayout.SOUTH
        );

        return outerPanel;
    }

    private void addField(
            JPanel panel,
            GridBagConstraints constraints,
            int row,
            String label,
            Component component
    ) {
        constraints.gridx = 0;
        constraints.gridy = row;
        constraints.fill =
                GridBagConstraints.NONE;
        constraints.weightx = 0.0;

        panel.add(
                new JLabel(label),
                constraints
        );

        constraints.gridx = 1;
        constraints.fill =
                GridBagConstraints.HORIZONTAL;
        constraints.weightx = 1.0;

        panel.add(
                component,
                constraints
        );
    }

    private void configureActions() {
        jobTypeComboBox.addActionListener(
                event -> updateInputFields()
        );

        loadCsvButton.addActionListener(
                event -> loadCsvFile()
        );

        submitButton.addActionListener(
                event -> submitJob()
        );
    }

    private void updateInputFields() {
        JobType selectedType =
                (JobType) jobTypeComboBox
                        .getSelectedItem();

        boolean rangeJob =
                selectedType == JobType.PRIMESUM;

        numbersArea.setEnabled(!rangeJob);
        rangeStartField.setEnabled(rangeJob);
        rangeEndField.setEnabled(rangeJob);
    }

    private void loadCsvFile() {
        JFileChooser fileChooser =
                new JFileChooser();

        fileChooser.setDialogTitle(
                "Select a CSV file"
        );

        fileChooser.setFileFilter(
                new FileNameExtensionFilter(
                        "CSV files",
                        "csv"
                )
        );

        int result =
                fileChooser.showOpenDialog(this);

        if (result != JFileChooser.APPROVE_OPTION) {
            return;
        }

        try {
            Path selectedFile =
                    fileChooser.getSelectedFile().toPath();

            String contents =
                    Files.readString(
                            selectedFile,
                            StandardCharsets.UTF_8
                    );

            List<Long> values =
                    parseNumbers(contents);

            JobType selectedType =
                    (JobType) jobTypeComboBox
                            .getSelectedItem();

            if (selectedType == JobType.PRIMESUM) {
                if (values.size() != 2) {
                    throw new IllegalArgumentException(
                            "A PRIMESUM CSV file must "
                                    + "contain exactly two numbers: "
                                    + "range start and range end"
                    );
                }

                rangeStartField.setText(
                        String.valueOf(values.get(0))
                );

                rangeEndField.setText(
                        String.valueOf(values.get(1))
                );
            } else {
                String joinedValues =
                        values.stream()
                                .map(String::valueOf)
                                .collect(
                                        Collectors.joining(", ")
                                );

                numbersArea.setText(joinedValues);
            }

            statusLabel.setText(
                    "Loaded CSV: "
                            + selectedFile.getFileName()
            );
        } catch (Exception exception) {
            showError(exception.getMessage());
        }
    }

    private void submitJob() {
        try {
            String host =
                    bootstrapHostField
                            .getText()
                            .trim();

            int port = parsePort(
                    bootstrapPortField.getText()
            );

            JobType jobType =
                    (JobType) jobTypeComboBox
                            .getSelectedItem();

            JobRequest request =
                    createJobRequest(jobType);

            int tableRow =
                    resultTableModel.getRowCount();

            resultTableModel.addRow(
                    new Object[]{
                            request.jobId(),
                            request.jobType(),
                            "Running",
                            0,
                            "",
                            "Submitted"
                    }
            );

            statusLabel.setText(
                    "Running job " + request.jobId()
            );

            executeJobInBackground(
                    host,
                    port,
                    request,
                    tableRow
            );
        } catch (Exception exception) {
            showError(exception.getMessage());
        }
    }

    private JobRequest createJobRequest(
            JobType jobType
    ) {
        if (jobType == null) {
            throw new IllegalArgumentException(
                    "Select a job type"
            );
        }

        String jobId =
                UUID.randomUUID().toString();

        if (jobType == JobType.PRIMESUM) {
            long rangeStart = parseLong(
                    rangeStartField.getText(),
                    "Range start"
            );

            long rangeEnd = parseLong(
                    rangeEndField.getText(),
                    "Range end"
            );

            if (rangeStart > rangeEnd) {
                throw new IllegalArgumentException(
                        "Range start cannot be greater "
                                + "than range end"
                );
            }

            return new JobRequest(
                    jobId,
                    jobType,
                    List.of(),
                    rangeStart,
                    rangeEnd
            );
        }

        List<Long> numbers =
                parseNumbers(numbersArea.getText());

        return new JobRequest(
                jobId,
                jobType,
                numbers,
                0L,
                0L
        );
    }

    private void executeJobInBackground(
            String host,
            int port,
            JobRequest request,
            int tableRow
    ) {
        SwingWorker<JobResult, JobProgress> worker =
                new SwingWorker<>() {
                    @Override
                    protected JobResult doInBackground()
                            throws Exception {
                        ClientConnection connection =
                                new ClientConnection(
                                        host,
                                        port
                                );

                        return connection.submitJob(
                                request,
                                progress -> publish(progress)
                        );
                    }

                    @Override
                    protected void process(
                            List<JobProgress> progressUpdates
                    ) {
                        if (progressUpdates.isEmpty()) {
                            return;
                        }

                        JobProgress latest =
                                progressUpdates.get(
                                        progressUpdates.size() - 1
                                );

                        updateProgressRow(
                                tableRow,
                                latest
                        );
                    }

                    @Override
                    protected void done() {
                        try {
                            JobResult result = get();

                            String status =
                                    result.successful()
                                            ? "Success"
                                            : "Failed";

                            String value =
                                    result.successful()
                                            ? String.valueOf(
                                            result.value()
                                    )
                                            : "";

                            updateResultRow(
                                    tableRow,
                                    status,
                                    value,
                                    result.message()
                            );

                            if (result.successful()) {
                                resultTableModel.setValueAt(
                                        100,
                                        tableRow,
                                        PROGRESS_COLUMN
                                );
                            }

                            statusLabel.setText(
                                    "Completed job "
                                            + result.jobId()
                            );
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();

                            updateResultRow(
                                    tableRow,
                                    "Interrupted",
                                    "",
                                    exception.getMessage()
                            );
                        } catch (ExecutionException exception) {
                            Throwable cause =
                                    exception.getCause();

                            String message =
                                    cause == null
                                            ? exception.getMessage()
                                            : cause.getMessage();

                            updateResultRow(
                                    tableRow,
                                    "Error",
                                    "",
                                    message
                            );

                            statusLabel.setText(
                                    "Job failed"
                            );
                        }
                    }
                };

        worker.execute();
    }

    private void updateProgressRow(
            int tableRow,
            JobProgress progress
    ) {
        latestJobProgress.put(
                progress.getJobId(),
                progress
        );

        int percentage = clampPercentage(
                progress.getOverallPercentage()
        );

        resultTableModel.setValueAt(
                formatEnumName(progress.getState().name()),
                tableRow,
                2
        );

        resultTableModel.setValueAt(
                percentage,
                tableRow,
                PROGRESS_COLUMN
        );

        if (!progress.getMessage().isBlank()) {
            resultTableModel.setValueAt(
                    progress.getMessage(),
                    tableRow,
                    5
            );
        }

        statusLabel.setText(
                "Job "
                        + progress.getJobId()
                        + ": "
                        + percentage
                        + "%"
        );
    }

    private void updateResultRow(
            int row,
            String status,
            String value,
            String message
    ) {
        resultTableModel.setValueAt(
                status,
                row,
                2
        );

        resultTableModel.setValueAt(
                value,
                row,
                4
        );

        resultTableModel.setValueAt(
                message == null ? "" : message,
                row,
                5
        );
    }

    private void showWorkerProgress(int modelRow) {
        String jobId = String.valueOf(
                resultTableModel.getValueAt(
                        modelRow,
                        0
                )
        );

        JobProgress progress =
                latestJobProgress.get(jobId);

        if (progress == null) {
            JOptionPane.showMessageDialog(
                    this,
                    "No worker-level progress has been received yet.",
                    "Worker Progress",
                    JOptionPane.INFORMATION_MESSAGE
            );
            return;
        }

        DefaultTableModel workerModel =
                new DefaultTableModel(
                        new Object[]{
                                "Worker",
                                "Task",
                                "State",
                                "Completed",
                                "Total",
                                "Progress",
                                "Partial Result",
                                "Message"
                        },
                        0
                ) {
                    @Override
                    public boolean isCellEditable(
                            int row,
                            int column
                    ) {
                        return false;
                    }
                };

        for (WorkerProgress workerProgress
                : progress.getWorkerProgressList()) {
            workerModel.addRow(
                    new Object[]{
                            workerProgress.getWorkerId(),
                            workerProgress.getTaskId(),
                            formatEnumName(
                                    workerProgress
                                            .getState()
                                            .name()
                            ),
                            workerProgress
                                    .getCompletedWorkUnits(),
                            workerProgress
                                    .getTotalWorkUnits(),
                            clampPercentage(
                                    workerProgress.getPercentage()
                            ) + "%",
                            workerProgress.getPartialValue(),
                            workerProgress.getMessage()
                    }
            );
        }

        JTable workerTable =
                new JTable(workerModel);

        workerTable.setFillsViewportHeight(true);
        workerTable.setAutoCreateRowSorter(true);

        JScrollPane scrollPane =
                new JScrollPane(workerTable);

        scrollPane.setPreferredSize(
                new Dimension(850, 250)
        );

        JOptionPane.showMessageDialog(
                this,
                scrollPane,
                "Worker progress - Job " + jobId,
                JOptionPane.INFORMATION_MESSAGE
        );
    }

    private static int clampPercentage(
            double percentage
    ) {
        return (int) Math.round(
                Math.max(
                        0.0,
                        Math.min(100.0, percentage)
                )
        );
    }

    private static String formatEnumName(String name) {
        return name
                .replace("JOB_STATE_", "")
                .replace("TASK_STATE_", "")
                .replace('_', ' ');
    }

    private List<Long> parseNumbers(
            String input
    ) {
        if (input == null || input.isBlank()) {
            throw new IllegalArgumentException(
                    "Enter at least one number"
            );
        }

        String[] tokens =
                input.trim().split("[,;\\s]+");

        List<Long> numbers =
                new ArrayList<>();

        for (String token : tokens) {
            if (!token.isBlank()) {
                numbers.add(
                        parseLong(token, "Number")
                );
            }
        }

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Enter at least one number"
            );
        }

        return numbers;
    }

    private long parseLong(
            String value,
            String fieldName
    ) {
        try {
            return Long.parseLong(value.trim());
        } catch (Exception exception) {
            throw new IllegalArgumentException(
                    fieldName
                            + " must contain a valid whole number"
            );
        }
    }

    private int parsePort(String value) {
        long port =
                parseLong(
                        value,
                        "Bootstrap port"
                );

        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException(
                    "Bootstrap port must be between "
                            + "1 and 65535"
            );
        }

        return (int) port;
    }

    private void showError(String message) {
        String safeMessage =
                message == null || message.isBlank()
                        ? "An unexpected error occurred"
                        : message;

        statusLabel.setText(safeMessage);

        JOptionPane.showMessageDialog(
                this,
                safeMessage,
                "DistriLab Error",
                JOptionPane.ERROR_MESSAGE
        );
    }

    private static final class ProgressBarRenderer
            extends JProgressBar
            implements TableCellRenderer {

        private ProgressBarRenderer() {
            super(0, 100);
            setStringPainted(true);
        }

        @Override
        public Component getTableCellRendererComponent(
                JTable table,
                Object value,
                boolean isSelected,
                boolean hasFocus,
                int row,
                int column
        ) {
            int percentage =
                    value instanceof Number number
                            ? clampPercentage(
                            number.doubleValue()
                    )
                            : 0;

            setValue(percentage);
            setString(percentage + "%");

            setBackground(
                    isSelected
                            ? table.getSelectionBackground()
                            : table.getBackground()
            );

            setForeground(
                    isSelected
                            ? table.getSelectionForeground()
                            : table.getForeground()
            );

            return this;
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            DistriLabClient client =
                    new DistriLabClient();

            client.setVisible(true);
        });
    }
}