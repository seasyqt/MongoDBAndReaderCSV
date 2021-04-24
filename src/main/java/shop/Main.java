package shop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {

  public static void main(String[] args) throws IOException {
    DBTools mongoDB = new DBTools();
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    while (true) {

      System.out.println("\nВ этой программе можно вести учет магазинов и товаров в них."
          + "\nМожно выполнить след. команды (Пример указан в скобках):"
          + "\n1.Добавить магазин (1 Название_магазина)"
          + "\n2.Добавить товар (2 Название_товара кол-во_товара)"
          + "\n3.Выставить товар в магазин (3 Название_товара название_магазина)"
          + "\n4.Вывести статистику товаров (4)"
          + "\n5.Выход");

      String line = reader.readLine();

      String[] inputConsole = line.split("\\s+");
      if (inputConsole[0].equals("5")) {
        break;
      }
      try {
        switch (inputConsole[0]) {
          case "1":
            mongoDB.addShop(inputConsole[1]);
            break;
          case "2":
            mongoDB.addProduct(inputConsole[1], Integer.parseInt(inputConsole[2]));
            break;
          case "3":
            mongoDB.putProductShop(inputConsole[1], inputConsole[2]);
            break;
          case "4":
            mongoDB.getStatisticsProducts();
            break;
          default:
            throw new IllegalArgumentException("Не верная команда");

        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}