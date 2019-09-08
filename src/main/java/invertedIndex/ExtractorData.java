package invertedIndex;

import java.io.Serializable;

/**
 * @Author bluesnail95
 * @Date 2019/7/18 23:22
 * @Description
 */
public class ExtractorData implements Serializable {

    private String link;

    private String id;

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
