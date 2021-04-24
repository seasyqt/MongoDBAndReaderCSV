package shop;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;
import org.bson.*;

public class DBTools {

  private final MongoCollection<Document> COLLECTION_PRODUCTS;
  private final MongoCollection<Document> COLLECTION_SHOPS;

  public DBTools() {
    MongoClient MONGO_CLIENT = new MongoClient("127.0.0.1", 27017);
    MongoDatabase DATABASE = MONGO_CLIENT.getDatabase("local");
    COLLECTION_PRODUCTS = DATABASE.getCollection("products");
    COLLECTION_SHOPS = DATABASE.getCollection("shops");
  }

  public MongoCollection<Document> getCollectionProducts() {
    return COLLECTION_PRODUCTS;
  }

  public MongoCollection<Document> getCollectionShops() {
    return COLLECTION_SHOPS;
  }

  public void addShop(String name) {

    getCollectionShops().insertOne(
        new Document("name", name).append("products", Collections.emptyList())
    );
    System.out.printf("\n------ Магазин <%s> добавлен в БД ------\n", name);
  }

  public void addProduct(String name, int price) {
    getCollectionProducts().insertOne(
        new Document("name", name)
            .append("price", price)
    );
    System.out.printf("\n------ Товар <%s> с ценой <%s> добавлен в БД ------\n", name, price);
  }

  public void putProductShop(String nameProduct, String nameShop) {
    StringBuilder whichShopUpdate = new StringBuilder()
        .append("{name : \"").append(nameShop).append("\"}");
    StringBuilder addProductInShop = new StringBuilder()
        .append("{$push : {products : \"").append(nameProduct).append("\"}}");
    getCollectionShops().updateOne(BsonDocument.parse(whichShopUpdate.toString())
        , BsonDocument.parse(addProductInShop.toString()));

    System.out
        .printf("\n------ Товар <%s> добавлен в магазин <%s> ------\n", nameProduct, nameShop);
  }

  public void getStatisticsProducts() {
    getAggregationResult().forEach((Consumer<Document>) doc -> {
      String shop = doc.getString("_id");
      int countCommon = doc.getInteger("countCommon");
      double avgPrice = doc.getDouble("avgPrice");
      int maxPrice = doc.getInteger("maxPrice");
      int minPrice = doc.getInteger("minPrice");
      int countLt100 = doc.getInteger("countLt");

      System.out.printf("----- Статистика по магазину <%s> -----\n"
              + "\tОбщее количество товаров - %s\n"
              + "\tСредняя цена товаров - %s\n"
              + "\tСамый дорогой товар - %s\n"
              + "\tСамый дешевый товар - %s\n"
              + "\tКоличество товаров дешевле 100 рублей - %s\n", shop, countCommon, avgPrice, maxPrice,
          minPrice, countLt100);
    });

  }


  public AggregateIterable<Document> getAggregationResult() {
    return getCollectionShops().aggregate(
        Arrays.asList(
            unwind("$products"),
            lookup("products", "products", "name", "products_list"),
            addFields(new Field<>("count", 1)),
            unwind("$products_list"),
            addFields(new Field<>("priceFirst", "$products_list.price")),
            project(fields(include("name", "products", "count", "priceFirst"))),
            group("$name",
                sum("count", "$count"),
                avg("avgPrice", "$priceFirst"),
                min("minPrice", "$priceFirst"),
                max("maxPrice", "$priceFirst"),
                push("products", "$priceFirst")),
            unwind("$products"),
            match(lt("products", 100)),
            addFields(new Field<>("countLt", 1)),
            group("$_id",
                sum("countLt", "$countLt"),
                first("countCommon", "$count"),
                first("avgPrice", "$avgPrice"),
                first("minPrice", "$minPrice"),
                first("maxPrice", "$maxPrice"))

        ));
  }


}