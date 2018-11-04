package Conversie;

public class ConvertUitvoerder {

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      Convert conversie = new Convert();
      conversie.convertGebruiker();
      conversie.convertPlaats();
      conversie.dummyKlant(); // maak een dummyklant aan om verweesde opdrachten aan te hangen
      conversie.convertKlant();
      conversie.convertOpdracht();
      conversie.close();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  
  }

}
