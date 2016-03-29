package dataengineering;

import javax.swing.JPanel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.PieSectionLabelGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.labels.StandardPieToolTipGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.text.DecimalFormat;

public class PieChart extends ApplicationFrame {
    private DefaultPieDataset dataset;
    private String title;

    public PieChart(String title) {
        super(title);
        this.title = title;
        dataset = new DefaultPieDataset();
        this.setSize(1080, 900);
        setContentPane(createDemoPanel());
        RefineryUtilities.centerFrameOnScreen(this);
    }

    public void addData(String label, Integer value) {
        this.dataset.setValue(label, value);
    }

    private  JFreeChart createChart(PieDataset dataset) {
        JFreeChart chart = ChartFactory.createPieChart(
               this.title,  // chart title
                dataset,        // data
                true,           // include legend
                true,
                false);
        final PiePlot plot1 = (PiePlot) chart.getPlot();
        PieSectionLabelGenerator labelGenerator = new StandardPieSectionLabelGenerator("{0} = {1}");

        StandardPieToolTipGenerator stip = new StandardPieToolTipGenerator(
                "{0}={2}",
                new DecimalFormat("#,##0"),
                new DecimalFormat("0%"));
        plot1.setToolTipGenerator(stip);

        StandardPieSectionLabelGenerator slbl = new StandardPieSectionLabelGenerator(
                "{0}={2}",
                new DecimalFormat("#,##0"),
                new DecimalFormat("0%"));
        plot1.setLabelGenerator(slbl);
        return chart;
    }

    public JPanel createDemoPanel() {
        JFreeChart chart = createChart(this.dataset);
        this.setVisible(true);
        return new ChartPanel(chart);
    }
}