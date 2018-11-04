package Conversie;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Convert {
  public static final String DBBRON = "betis"; // X-oud
  public static final String DBDOEL = "D://OP//Opdracht_05_ROB.fdb"; //Y-nieuw
  public static final String DOELUSER = "sysdba";
  public static final String DOELPASSWORD = "masterkey";
  public static final String DOELDRIVERNAME = "org.firebirdsql.jdbc.FBDriver";
  public static final String BRONURL = "jdbc:mysql://localhost/" + DBBRON;
  public static final String BRONUSER = "betis";
  public static final String BRONPASSWORD = "betis";
  public static final String BRONDRIVERNAME = "com.mysql.jdbc.Driver";
  public static final String DOELURL = "jdbc:firebirdsql:localhost:" + DBDOEL;
  public static final String CONVERTDATE = "2017-04-02"; // door de aanroeper van het programma in te stellen datum voor alle velden die in systeem Y-nieuw een datum nodig hebben, maar waarvoor in systeem X-oud geen datum bekend is 
  
  private Connection cbron, cdoel = null;
  private PreparedStatement bronps, doelps = null;

  /**
   * laad de drivers voor firebird en mysql databases
   * en maakt verbinding met de databases X-oud (cbron) en Y-nieuw (cdoel)
   */  
  public Convert() throws Exception {
    Class.forName("org.firebirdsql.jdbc.FBDriver");
    Class.forName("com.mysql.jdbc.Driver");
    System.out.println("drivers geladen");
    cbron = DriverManager.getConnection(BRONURL, BRONUSER, BRONPASSWORD);
    System.out.println("Connection Mysql gemaakt");
    cdoel = DriverManager.getConnection(DOELURL, DOELUSER, DOELPASSWORD);
    System.out.println("Connection Firebird gemaakt");
    
  }
  
  /**
   * leest inhoud van tabel Gebruiker uit systeem X oud op, 
   * en voegt de benodigde inhoud toe in systeem Y nieuw in tabel Medewerker
   */  
  public void convertGebruiker() throws Exception {
    bronps = cbron.prepareStatement("select naam from Gebruiker");
    doelps = cdoel.prepareStatement("insert into Medewerker values (?)");
    ResultSet rs = bronps.executeQuery();
    while (rs.next()) {
      String naam = rs.getString("naam");
      System.out.println(naam);
      doelps.setString(1, naam);
      doelps.executeUpdate();
    }
    System.out.println("Tabel Gebruiker geconverteerd");
  }
  
  /**
   * leest inhoud van tabel Plaats uit systeem X oud op, 
   * en voegt de benodigde inhoud toe in systeem Y nieuw in tabel Plaats
   */  
  public void convertPlaats() throws Exception {
    bronps = cbron.prepareStatement("select naam from Plaats");
    doelps = cdoel.prepareStatement("insert into Plaats values (?)");
    ResultSet rs = bronps.executeQuery();
    while (rs.next()) {
      String naam = rs.getString("naam");
      System.out.println(naam);
      doelps.setString(1, naam);
      doelps.executeUpdate();
    }
    System.out.println("Tabel Plaats geconverteerd");
  }
  
  /** 
   * haal e-mail uit notitie, indien aanwezig en koppel e-mail adres als Nfa-Type aan Klant in systeem Y-nieuw
   * geef notitie min e-mail weer terug voor verdere verwerking
   */
  public String voegEmailToe(int nummer, String notitie) throws Exception {
    String notitieruw = notitie;
    if (notitieruw==null || notitieruw.isEmpty()) {
      System.out.println("email onbekend");      
    }
    else {
      String email = null ;
      notitieruw = notitieruw.trim();
      Matcher m = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+").matcher(notitieruw);
      while (m.find()) {
          email = m.group();
      }
      if (email != null && !email.isEmpty()) {
          notitieruw = notitieruw.substring(email.length()); 
          System.out.println("gevonden mailadres: " + email);
          doelps = cdoel.prepareStatement("insert into Klant_Nfa (klant, nfa_type, nummer_adres)  values (?, ?, ?)");
          doelps.setInt(1, nummer);
          doelps.setString(2, "e-mail"); // wijzig deze waarde als we een andere naam voor e-mail type willen
          doelps.setString(3, email);      
          doelps.executeUpdate();  
      }    
    }
    return notitieruw;
  }
  
  /**
   *  maak NFA met telefoonnummer(s) aan bij Klant in systeem Y-nieuw
   * 
   */
  public void voegTelefoonToe (int nummer, String telefoonruw) throws Exception {
    if ((telefoonruw==null) || (telefoonruw.isEmpty())) {
      System.out.println("telefoon onbekend" );
    }
    else {
      //eerste nummmer
      telefoonruw = telefoonruw.trim();
      Matcher m = Pattern.compile("[0-9\\-\\+]{8,14}").matcher(telefoonruw);
      if (m.find()) {
          String telefoon1 = m.group();
          if (telefoonruw.length()>10){
            telefoonruw = telefoonruw.substring(telefoon1.length());             
          }
          System.out.println("met telefoon: " + telefoon1 );
          doelps = cdoel.prepareStatement("insert into Klant_Nfa (klant, nfa_type, nummer_adres)  values (?, ?, ?)");
          doelps.setInt(1, nummer);
          doelps.setString(2, "telefoon"); // wijzig deze waarde als we een andere naam voor e-mail type willen
          doelps.setString(3, telefoon1); 
          doelps.executeUpdate();
      }
      // eventueel tweede nummer
      if (telefoonruw.length()>10){
        telefoonruw = telefoonruw.trim();
        Matcher m2 = Pattern.compile("[0-9\\-\\+]{8,14}").matcher(telefoonruw);
        if (m2.find()) {
            String telefoon2 = m2.group();
            System.out.println("met tweede telefoon: " + telefoon2 );
            doelps = cdoel.prepareStatement("insert into Klant_Nfa (klant, nfa_type, nummer_adres, omschrijving)  values (?, ?, ?, ?)");
            doelps.setInt(1, nummer);
            doelps.setString(2, "telefoon"); // wijzig deze waarde als we een andere naam voor e-mail type willen
            doelps.setString(3, telefoon2);
            doelps.setString(4, "bij geen gehoor / alternatief");
            doelps.executeUpdate();
        }
      }        
    }
  }
  
  
 
  

  /**
   * maak van notitie voor blokkering een geldige blokkeringsreden
   * 
   */
  public String setBlokkeringsreden(String reden) throws Exception {
    String blokkeringsreden = "overig"; // default return waarde, kan op basis van onderstaande iets anders worden
    if (reden.equals("Verhuisd") ) {
      blokkeringsreden = "verhuisd buiten regio";
    }
    if (reden.equals("Probleem") ) {
      blokkeringsreden = "probleem klant";
    }
    if (reden.equals("finan") ) {
      blokkeringsreden = "financiële problemen";
    }
    if (reden.equals("finan") ) {
      blokkeringsreden = "opgeheven";
    }
    return blokkeringsreden;
  }
  
  /**
   *  maak blokkeringreden aan indien van toepassing bij Klant in systeem Y-nieuw
   * 
   */
  public void blokkeerKlant (int nummer, String geblokkeerd, String notitieruw) throws Exception {
    if (geblokkeerd.equals("J")) {
      String blokkeringsreden = null;
      notitieruw = notitieruw.trim();
      if (notitieruw==null || notitieruw.isEmpty()) {
        blokkeringsreden = "overig";
      }
      else {
        blokkeringsreden = setBlokkeringsreden(notitieruw);
      }      
      System.out.println("Klant is geblokkeerd om reden: " + blokkeringsreden);
      
      doelps = cdoel.prepareStatement("Update Klant set blokkeringsreden = ?, datum_blokkering = ? where nr = ?");
      doelps.setString(1, blokkeringsreden);
      doelps.setDate(2, java.sql.Date.valueOf(CONVERTDATE)); // default datum voor blokkeringen uit het verleden
      doelps.setInt(3, nummer);
      doelps.executeUpdate();
    }
  }
  
  
  /**
   *  vul notitie bij Klant in systeem Y-nieuw
   *  als klant niet geblokkeerd is en er nog iets over is na e-mail verwijderen
   * 
   */ 
  public void vulNotitie(int nummer, String geblokkeerd, String notitieruw) throws Exception {
    String medewerker = "dev";  // default medewerker die alle nodige notities uit systeem X-oud in systeem Y-nieuw voor zijn rekening neemt
    if (geblokkeerd.equals("N")) {
      if (notitieruw==null || notitieruw.isEmpty()) {
        System.out.println("Klant heeft geen notitie");
      }
      else {
        notitieruw = notitieruw.trim();
        String notitie = notitieruw;
        System.out.println("Klant heeft notitie: " + notitie);
        doelps = cdoel.prepareStatement("insert into Klantnotitie (klant, datum_tijd, medewerker, tekst) values (?, ?, ?, ?)");
        doelps.setInt(1, nummer);
        doelps.setDate(2, java.sql.Date.valueOf(CONVERTDATE)); // default datum voor notitie's uit het verleden
        doelps.setString(3, medewerker);
        doelps.setString(4, notitie);
        doelps.executeUpdate();
      }
    }
   }
  
  
  /**
   * achterhaal velden contactpersoon uit cp en voeg deze toe als contactpersoon bij Klant
   */
  public void voegContactPersoonToe (int nummer, String contactpersoonruw) throws Exception {
    if ((contactpersoonruw==null) || (contactpersoonruw.isEmpty())) {
      System.out.println("Contactpersoon onbekend");
    }
    else {
      // maak tijdelijke variabelen aan voor conversie van contactpersoon
      String geslachtString = null;
      String geslacht = null;
      String voornaam = null;
      String voorletters = null;
      String tussenvoegsel = null;
      String achternaam = null;
      //vul waarden voor deze persoon - eerst geslacht uit naam proberen te trekken
      contactpersoonruw = contactpersoonruw.trim();
      Matcher m = Pattern.compile("DHR[\\.]|Dhr[\\.]|DE HEER").matcher(contactpersoonruw);
      while (m.find()) {
          geslachtString = m.group();
          geslacht = "M";
      }
      if (geslachtString != null && !geslachtString.isEmpty()) {
        contactpersoonruw = contactpersoonruw.substring(geslachtString.length());
      }
      achternaam = contactpersoonruw.trim();
      System.out.println("Klant heeft contactpersoon: " + geslacht + voornaam + voorletters + tussenvoegsel + achternaam);
      doelps = cdoel.prepareStatement("insert into Contactpersoon (klant, geslacht, voornaam, voorletters, tussenvoegsel, achternaam) values (?, ?, ?, ?, ?, ?)");
      doelps.setInt(1, nummer);
      doelps.setString(2, geslacht);
      doelps.setString(3, voornaam);
      doelps.setString(4, voorletters);
      doelps.setString(5, tussenvoegsel);
      doelps.setString(6, achternaam);
      doelps.executeUpdate();
    }
  }

  public void voegPlaatsToe(String plaatsnaam) throws Exception {
    try {
      doelps = cdoel.prepareStatement("insert into Plaats values (?)");
      doelps.setString(1, plaatsnaam);
      doelps.executeUpdate();
    }
    catch (SQLException e) {
      System.out.println("plaats bestaat al");
    }
  }

  public void voegMedewerkerToe(String medewerker) throws Exception {
    try {
      doelps = cdoel.prepareStatement("insert into Medewerker values (?)");
      doelps.setString(1, medewerker);
      doelps.executeUpdate();
    }
    catch (SQLException e) {
      System.out.println("medewerker bestaat al");
    }
  }
  
  
  /**
   * leest inhoud van tabel Klant uit systeem X oud op, converteert waar nodig 
   * en voegt de benodigde inhoud toe in systeem Y nieuw
   */  
  public void convertKlant() throws Exception {
    bronps = cbron.prepareStatement("select * from Klant");
    ResultSet rs = bronps.executeQuery();
    while (rs.next()) {
      // haal waarden uit systeem X oud op
      int nummer = rs.getInt("nr");
      String klantnaam = rs.getString("naam");
      String straatnaam = rs.getString("straat");
      String huisnummer = rs.getString("huisnr");
      String postcode = rs.getString("pc");
      String plaatsnaam = rs.getString("plaats");
      String telefoonruw = rs.getString("tel");
      String notitieruw = rs.getString("notitie");
      String geblokkeerd = rs.getString("blok");
      String contactpersoonruw = rs.getString("cp");


      // maak nieuwe klant aan met waarden
      System.out.println("klant: " + nummer + ";" + klantnaam + ";" + huisnummer + ";" + postcode + ";" + plaatsnaam + ";" + geblokkeerd + ";" );
      doelps = cdoel.prepareStatement("insert into Klant (nr, naam, straat, huisnr, postcode, plaats, geblokkeerd)  values (?, ?, ?, ?, ?, ?, ?)");
      doelps.setInt(1, nummer);
      doelps.setString(2, klantnaam);
      doelps.setString(3, straatnaam);
      doelps.setString(4, huisnummer);
      doelps.setString(5, postcode);
      doelps.setString(6, plaatsnaam);
      doelps.setString(7, geblokkeerd);
      doelps.executeUpdate();     
      
      // doe onderstaande aanvullende handelingen om een klant juist in systeem Y-nieuw te registreren
      notitieruw = voegEmailToe(nummer, notitieruw); // haal e-mail adres indien aanwezig uit notitie en sla e-mail op als Klant-Nfa - belangrijk, deze functie moet als eerste van onderstaande aangeroepen worden
      voegTelefoonToe(nummer, telefoonruw); // voeg indien aanwezig 1 of 2 telefoonnummers aan Klant_Nfa toe
      blokkeerKlant(nummer, geblokkeerd, notitieruw); // indien klant geblokkeerd moet worden, gebeurt dit en wordt een reden toegevoegd
      vulNotitie(nummer, geblokkeerd, notitieruw); // indien klant niet geblokkeerd is, probeer dan of er een notitie aangemaakt moet worden
      voegContactPersoonToe(nummer, contactpersoonruw); // indien er een contactpersoon bekend is, voeg deze dan in het juiste format toe

    }
    System.out.println("Tabel Klant geconverteerd");
  }
  
  
  /**
   * controleer of er in deze notitie iets staat waardoor vanaf en naar adres moeten worden omgedraaid
   * @param notitie
   * @return
   * @throws Exception
   */
  public boolean checkOmkeer (String notitie) throws Exception {
    if (notitie!=null && !notitie.isEmpty()) {
      notitie = notitie.trim();
      Matcher m = Pattern.compile("bezorgen bij klant | klant is afleveradres | afleveren bij klant").matcher(notitie);
      if (m.find()) {
        return true;
      }
    }
    return false;
  }
  
  
  /**
   * controleer of er in deze notitie iets over spoed staat
   * @param notitie
   * @return
   * @throws Exception
   */
  public boolean checkSpoed(String notitie) throws Exception {
    if (notitie!=null && !notitie.isEmpty()) {
      notitie = notitie.trim();
      Matcher m = Pattern.compile("spoed | SPOED").matcher(notitie);
      if (m.find()) {
        return true;
      }
    }
    return false;      
  }
  
  
  /**
   * controleer of er in deze notitie iets over een annulering staat
   * @param notitie
   * @return
   * @throws Exception
   */
  public boolean checkAnnulering(String notitie) throws Exception {
    if (notitie!=null && !notitie.isEmpty()) {
      notitie = notitie.trim();
      Matcher m = Pattern.compile("geannuleerd | SPOED").matcher(notitie);
      if (m.find()) {
        return true;
      }
    }
    return false;   
  }
  
  /**
   * voeg een opdrachtNotitie bij de betreffende opdracht toe
   * @param nummer
   * @param medewerker
   * @param notitie
   * @param opdracht_datum
   * @throws Exception
   */
  public void voegOpdrachtNotitieToe(int nummer, String medewerker, String notitie, Date opdracht_datum) throws Exception {
    if (notitie==null || notitie.isEmpty()) {
      System.out.println("Opdracht heeft geen notitie");
    }
    else {
      notitie = notitie.trim();
      // TODO: achterhaal alternatieve datum in notitiewaarde
      // TODO: achterhaal alternatieve medewerker in notitiewaarde
      System.out.println("Opdracht heeft notitie: " + notitie);
      doelps = cdoel.prepareStatement("insert into Opdrachtnotitie (opdracht, datum_tijd, medewerker, tekst) values (?, ?, ?, ?)");
      doelps.setInt(1, nummer);
      doelps.setDate(2, opdracht_datum); // default datum voor notitie's uit het verleden
      doelps.setString(3, medewerker);
      doelps.setString(4, notitie);
      doelps.executeUpdate();
    }
  }
  
  /**
   * voeg een dummy klant op nummer 0 toe om verweesde opdrachten aan te hangen
   */
  
  public void dummyKlant() throws Exception{
    doelps = cdoel.prepareStatement("insert into Klant (nr, naam, straat, huisnr, postcode, plaats, geblokkeerd)  values (0, 'dummy', 'dummystraat', 1, '0000AA', 'Amsterdam', 'J')");
    doelps.executeUpdate(); 
  }
  
  
  
  /**
   * leest inhoud van tabel Opdracht uit systeem X oud op, converteert waar nodig 
   * en voegt de benodigde inhoud toe in systeem Y nieuw
   */  
  public void convertOpdracht() throws Exception {
    // conversie in blokken van 5000 om gebruikte geheugengrootte te beperken - aantal in tabel is 1778104 dus tot 1775000
    for (int i=0;i<=1775000;i=i+5000){
      bronps = cbron.prepareStatement("select * from Opdracht where nr > ? and nr < ?"); // aantal op basis van for loop, tot en met nummer 2284 gekomen
      bronps.setInt(1, i);     
      bronps.setInt(2, (i+5000));
      ResultSet rs = bronps.executeQuery();
      while (rs.next()) {
        // haal waarden uit systeem X oud op
        int nummer = rs.getInt("nr");
        Date datum_opdracht = rs.getDate("dopdr");
        int klantnummer = rs.getInt("klantnr");
        int aantal_colli = rs.getInt("colli");
        int gewicht = rs.getInt("kg");
        String naar_straatnaam = rs.getString("straat");
        String naar_huisnr = rs.getString("huisnr");
        String naar_postcode = rs.getString("pc");
        String naar_plaatsnaam = rs.getString("plaats");
        Date datum_planning = rs.getDate("dplan");
        Date datum_transport = rs.getDate("dtrans");
        String bon_ontvangen = rs.getString("BonBin");
        String medewerker = rs.getString("Mdw");
        Float bedrag = rs.getFloat("Bedrag");
        String notitieruw = rs.getString("notitie");
  
        // haal vanaf adres van deze betreffende klant op
        ResultSet rsvan;
        try {
          bronps = cbron.prepareStatement("select * from Klant Where nr=?");
          bronps.setInt(1, klantnummer);
          rsvan = bronps.executeQuery();
          rsvan.next();
        }
        catch (SQLException e) {
          klantnummer = 0;
          bronps = cbron.prepareStatement("select * from Klant Where nr=?");
          bronps.setInt(1, klantnummer);
          rsvan = bronps.executeQuery();
          rsvan.next();
        }        
        String van_straatnaam = rsvan.getString("straat");
        String van_huisnr = rsvan.getString("huisnr");
        String van_postcode = rsvan.getString("pc");
        String van_plaatsnaam = rsvan.getString("plaats");
        
        
        // maak default waarden voor spoed en geannuleerd aan
        String spoed = "N";
        String geannuleerd = "N";
  
  
        // probeer voor de zekerheid eerst bestemmingsplaats in te voeren bij tabel Plaats en medewerker bij tabel Medewerker
        voegPlaatsToe (naar_plaatsnaam);
        voegMedewerkerToe (medewerker);
        
        // controleer of vanaf en naar adressen moeten worden omgedraaid op basis van notitie
        if (checkOmkeer(notitieruw)) {
          String temp_straatnaam = van_straatnaam;
          String temp_huisnr = van_huisnr;
          String temp_postcode = van_postcode;
          String temp_plaatsnaam = van_plaatsnaam;
          van_straatnaam = naar_straatnaam;
          van_huisnr = naar_huisnr;
          van_postcode = naar_postcode;
          van_plaatsnaam = naar_plaatsnaam;
          naar_straatnaam = temp_straatnaam;
          naar_huisnr = temp_huisnr;
          naar_postcode = temp_postcode;
          naar_plaatsnaam = temp_plaatsnaam;
          System.out.println("van en naar omgedraaid");
        }
        // controleer of opdracht spoed heeft
        if (checkSpoed(notitieruw)) {
          spoed = "J";
        }
        
        // controleer of opdracht geannuleerd is
        if (checkAnnulering(notitieruw)) {
          geannuleerd = "J";
        }
  
        
        // maak nieuwe opdracht aan met waarden
        System.out.println("opdracht: " + nummer + "; voor" + klantnummer + "op datum;" + datum_opdracht + " aantal en gewicht; " + aantal_colli + gewicht + "; transport en planning" + datum_transport + ";" + datum_planning + ";" + bon_ontvangen + ";" + medewerker + ";" +bedrag );
        System.out.println("van adres: " + van_straatnaam + ";"+ van_huisnr + ";" + van_postcode + ";" + van_plaatsnaam );
        System.out.println("naar adres: " + naar_straatnaam + ";" + naar_huisnr + ";" + naar_postcode + ";" + naar_plaatsnaam);
        doelps = cdoel.prepareStatement("insert into Opdracht (nr, datum_opdracht, klant, aantal_colli, gewicht, spoed_, van_straat, van_huisnr, van_postcode, van_plaats, naar_straat, naar_huisnr, naar_postcode, naar_plaats, datum_planning, datum_transport, geannuleerd, bon_ontvangen_, medewerker, bedrag)  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        doelps.setInt(1, nummer);
        doelps.setDate(2, datum_opdracht);
        doelps.setInt(3, klantnummer);
        doelps.setInt(4, aantal_colli);
        doelps.setInt(5, gewicht);
        doelps.setString(6, spoed);     
        doelps.setString(7, van_straatnaam);
        doelps.setString(8, van_huisnr);
        doelps.setString(9, van_postcode);
        doelps.setString(10, van_plaatsnaam);
        doelps.setString(11, naar_straatnaam);
        doelps.setString(12, naar_huisnr);
        doelps.setString(13, naar_postcode);
        doelps.setString(14, naar_plaatsnaam);     
        doelps.setDate(15, datum_planning);
        doelps.setDate(16, datum_transport);
        doelps.setString(17, geannuleerd); 
        doelps.setString(18, bon_ontvangen);   
        doelps.setString(19, medewerker);  
        doelps.setFloat(20, bedrag);
        doelps.executeUpdate();     
        
        // doe onderstaande aanvullende handelingen om een opdracht juist in systeem Y-nieuw te registreren
        voegOpdrachtNotitieToe(nummer, medewerker, notitieruw, datum_opdracht);
  
      }
      System.out.println("Tabel Opdracht geconverteerd tot" + i);
    }
  }
  
  
  
  /**
   * sluit de verbinding met de databases af
   */
  
  public void close() throws Exception {
    if (cbron!=null) {
      cbron.close();
    }
    if (cdoel!=null) {
      cdoel.close();
    }
  }
  
  

}
