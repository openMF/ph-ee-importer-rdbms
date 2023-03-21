package hu.dpc.phee.operator.config;

public class BpmnProcess {

    private String id, direction, type;

    public BpmnProcess() {
    }

    public BpmnProcess(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "BpmnProcess{" +
                "id='" + id + '\'' +
                ", direction='" + direction + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
