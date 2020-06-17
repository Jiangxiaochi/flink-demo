package xc.flink.datastream;

public class StatisticData {

    private String lineId;
    private String productId;
    private Integer count;

    public String getLineId() {
        return lineId;
    }

    public void setLineId(String lineId) {
        this.lineId = lineId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "StatisticData{" +
                "lineId='" + lineId + '\'' +
                ", productId='" + productId + '\'' +
                ", count=" + count +
                '}';
    }
}
