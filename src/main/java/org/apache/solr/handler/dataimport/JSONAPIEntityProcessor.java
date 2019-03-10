package org.apache.solr.handler.dataimport;

import java.util.ListIterator;
import java.util.Map;

public class JSONAPIEntityProcessor extends EntityProcessorBase {
    private String url;
    private ListIterator<Map<String, Object>> rowIterator;

    public static final String URL = "url";

    public JSONAPIEntityProcessor() {
    }

    @Override
    public void init(Context context) {
        super.init(context);

        url = context.getResolvedEntityAttribute(URL);
        if (url == null) {
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE, "'" + URL + "' is a required attribute");
        }

        rowIterator = (ListIterator) context.getDataSource().getData(url);
    }

    @Override
    public Map<String, Object> nextRow() {
        if (!rowIterator.hasNext()) {
            return null;
        }
        return rowIterator.next();
    }
}
