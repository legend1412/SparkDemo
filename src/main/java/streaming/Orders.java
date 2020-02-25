package streaming;

public class Orders {
    private String order_id;
    private String user_id;
    private String eval_set;
    private String order_number;
    private String order_dow;
    private String hour;
    private String day;

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getEval_set() {
        return eval_set;
    }

    public void setEval_set(String eval_set) {
        this.eval_set = eval_set;
    }

    public String getOrder_number() {
        return order_number;
    }

    public void setOrder_number(String order_number) {
        this.order_number = order_number;
    }

    public String getOrder_dow() {
        return order_dow;
    }

    public void setOrder_dow(String order_dow) {
        this.order_dow = order_dow;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
