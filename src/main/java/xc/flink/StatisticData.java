package xc.flink;

public class StatisticData {

    private static transient final long serialVersionUID = -7644778048949666045L;
    private String productId;
    private Integer count;

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
                "productId='" + productId + '\'' +
                ", count=" + count +
                '}';
    }

}
