import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.FileReader;
import org.bson.BsonDocument;
import org.bson.Document;

public class ReaderCSV {

  private static final String CSV_FILE = "src/main/resources/mongo.csv";
  private static final MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

  public static void main(String[] args) {
    MongoDatabase database = mongoClient.getDatabase("local");
    MongoCollection<Document> collection = database.getCollection("students");
    collection.drop();
    ReaderCSV csv = new ReaderCSV();
    csv.run(collection);
    System.out.println("Общее количество студентов в базе - " + collection.countDocuments());
    System.out.println("Количество студентов старше 40 лет - " + collection
        .countDocuments((BsonDocument.parse("{ \"age\": { $gt: \"40\" } } "))));
    System.out.println("Имя самого молодого студента - " + collection.find()
        .sort(BsonDocument.parse("{age: 1}")).limit(1).iterator().next().getString("name"));
    System.out.println("Cписок курсов самого старого студента - " + collection.find()
        .sort(BsonDocument.parse("{age: -1}")).limit(1).iterator().next().getString("courses"));
    String minAgeStudent = collection.find()
        .sort(BsonDocument.parse("{age: 1}"))
        .limit(1).iterator().next().getString("age");
    String maxAgeStudent = collection.find()
        .sort(BsonDocument.parse("{age: -1}")).limit(1).iterator().next().getString("age");
    String bsonDoc = "{\"age\": \"%s\"}";

    System.out.println("\nВсе самые молодые студенты ");
    collection.find(BsonDocument.parse(String.format(bsonDoc, minAgeStudent))).forEach(
        (Block<? super Document>) doc ->
            System.out.println("Имя "
                + doc.getString("name")
                + " | Возраст "
                + doc.getString("age"))
    );
    System.out.println("\nВсе самые старые студенты ");
    collection.find(BsonDocument.parse(String.format(bsonDoc, maxAgeStudent))).forEach(
        (Block<? super Document>) doc ->
            System.out.println("Имя "
                + doc.getString("name")
                + " | Возраст "
                + doc.getString("courses"))
    );


  }

  public void run(MongoCollection<Document> collection) {
    String line = "";

    try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE))) {

      while ((line = br.readLine()) != null) {
        String[] students = line.split(",", 3);
        Document document = new Document()
            .append("name", students[0])
            .append("age", students[1])
            .append("courses", students[2].replace("\"", ""));
        collection.insertOne(document);

      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}