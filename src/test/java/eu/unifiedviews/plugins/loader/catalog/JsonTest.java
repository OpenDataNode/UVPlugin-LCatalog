package eu.unifiedviews.plugins.loader.catalog;

import java.util.Collections;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import eu.unifiedviews.helpers.dataunit.resourcehelper.Resource;
import eu.unifiedviews.helpers.dataunit.resourcehelper.ResourceConverter;

public class JsonTest {

    @Test
    public void test() throws JSONException {
        Resource resource = new Resource();
        resource.getExtras().getMap().put("dsfafds", "Fdsfd");
        resource.setName("Fsdfds");
        resource.setMimetype(null);

        JSONObject resourceExtras = new JSONObject(ResourceConverter.extrasToMap(resource.getExtras()));

        JSONObject resourceEntity = new JSONObject(ResourceConverter.resourceToMap(resource));
        resourceEntity.put("extras", resourceExtras);

        System.out.println(resourceEntity.toString());
    }

    @Test
    public void test2() {
        Resource resource = new Resource();
        resource.getExtras().getMap().put("dsfafds", "Fdsfd");
        resource.setName("Fsdfds");
        resource.setMimetype(null);

        JsonBuilderFactory factory = Json.createBuilderFactory(Collections.<String, Object> emptyMap());
        JsonObjectBuilder resourceExtrasBuilder = factory.createObjectBuilder();
        for (Map.Entry<String, String> mapEntry : ResourceConverter.extrasToMap(resource.getExtras()).entrySet()) {
            resourceExtrasBuilder.add(mapEntry.getKey(), mapEntry.getValue());
        }

        JsonObjectBuilder resourceBuilder = factory.createObjectBuilder();
        for (Map.Entry<String, String> mapEntry : ResourceConverter.resourceToMap(resource).entrySet()) {
            resourceBuilder.add(mapEntry.getKey(), mapEntry.getValue());
        }

        resourceBuilder.add("extras", resourceExtrasBuilder);

        System.out.println(resourceBuilder.build().toString());
    }
}
