package eu.unifiedviews.plugins.loader.catalog;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import eu.unifiedviews.helpers.dataunit.resourcehelper.Resource;
import eu.unifiedviews.helpers.dataunit.resourcehelper.ResourceConverter;

public class JsonTest {

    @Test
    public void test() throws JSONException {
        Resource resource = new Resource();
        resource.getExtras().put("dsfafds", "Fdsfd");
        resource.setName("Fsdfds");
        resource.setMimetype(null);

        JSONObject resourceExtras = new JSONObject(resource.getExtras());

        JSONObject resourceEntity = new JSONObject(ResourceConverter.toMap(resource));
        resourceEntity.put("extras", resourceExtras);

        System.out.println(resourceEntity.toString());
    }
}
