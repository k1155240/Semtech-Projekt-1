import java.text.MessageFormat;
import java.util.InputMismatchException;
import java.util.Scanner;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;

public class Project {
	private static Dataset dataset;
	public static void main(String[] args) {      
		/* open (and create if not exists) a TDB database */
		//String directory = "C:/Users/wolfsst/workspace/Einheit2/src/SemTech_MiniProject/db";
		String directory = "F:/Dropbox/Studium/Master/2. Semester/Semantische Technologien/Mini Projekt 1/db"; //CHANGE TO A DIRECTORY ON YOUR FILE-SYSTEM
		dataset = TDBFactory.createDataset(directory);

		System.out.println("\n\n------------------------------");
		System.out.println("SemTech Mini Project");
		System.out.println("Authors: Stefan | Max");
		System.out.println("------------------------------");

		FillCompanyGraph(); 
		
		boolean run = true;
		while(run) {
			try {
			run = doAction();
			}
			catch(InputMismatchException ex) {
				System.out.println("Invalid input");
			}
		}
	}

	private static void FillCompanyGraph() {
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			dataset.getNamedModel("Companies").removeAll();
			dataset.commit();
		} finally { dataset.end(); } // END TRANSACTION (ABORT IF NO COMMIT)
		
		String insertQuery = 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
						"PREFIX : <http://example.org/>\n" +
						"PREFIX wd: <http://www.wikidata.org/entity/>\n"+
						"INSERT DATA { GRAPH :Companies {\n" + 
						":microsoft :name \"Microsoft\"; :wikidata wd:Q2283.\n" +
						":apple :name \"Apple\"; :wikidata wd:Q312.\n" +
						":ibm :name \"IBM\"; :wikidata wd:Q37156.\n" +
						":amazon :name \"Amazon\"; :wikidata wd:Q3884.\n" +
						"}}";

		UpdateRequest request = UpdateFactory.create(insertQuery);
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			UpdateAction.execute(request, dataset) ;
			dataset.commit();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		}
	}

	private static boolean doAction() {
		System.out.println("\n------------------------------");
		System.out.println("Actions:");
		System.out.println("0 - Exit");
		System.out.println("1 - Show all persons");
		System.out.println("2 - Find persons");
		System.out.println("3 - Show specific person");
		System.out.println("4 - Insert new person");
		System.out.println("5 - Change person");
		System.out.println("6 - Delete person");
		System.out.println("7 - Show deleted persons");
		System.out.println("------------------------------");

		System.out.println("------------------------------");
		System.out.print("What do you want to do: ");

		Scanner sc = new Scanner(System.in);
		int action = sc.nextInt();
		sc.nextLine();

		System.out.println("------------------------------");
		System.out.println("You chose " + action);
		System.out.println("------------------------------");

		switch (action) {
		case 0:
			System.out.println("Exiting!");
			return false;
		case 1:
			showAll();
			break;
		case 2:
			findPersons();
			break;
		case 3:
			showSpecific();
			break;
		case 4:
			insertPerson();
			break;
		case 5:
			changePerson();
			break;
		case 6:
			deletePerson();
			break;
		case 7:
			showDeleted();
			break;
		default:
			System.out.println("Invalid input!");
			break;
		}

		return true;
	}

	private static void findPersons() {
		System.out.println("\n\n------------------------------");
		System.out.print("Find persons");
		System.out.println("Actions:");
		System.out.println("1 - Find by gender");
		System.out.println("2 - Find by address");
		System.out.print("Select action: ");
		
		Scanner sc = new Scanner(System.in);
		int  action = sc.nextInt();
		sc.nextLine();
		
		switch (action) {
		case 1:
			findPersonsByGender();
			break;
		case 2:
			findPersonsByAddress();
			break;
		default:
			System.out.println("Invalid input!");
			return;
		}
	}

	private static void findPersonsByAddress() {
		System.out.print("Address: ");
		Scanner sc = new Scanner(System.in);
		String address = sc.nextLine();
		
		String queryStr =             
				MessageFormat.format("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"+
						"PREFIX : <http://example.org/>\n"+
						"PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>\n"+
						"SELECT ?personid WHERE '{' ?personid a :Person; :address ?ad. FILTER contains(?ad,\"{0}\")'}'", address);

		Query query = QueryFactory.create(queryStr);
		dataset.begin(ReadWrite.READ); // START TRANSACTION
		try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
			ResultSet results = qexec.execSelect() ;
			ResultSetFormatter.out(System.out, results, query) ;
		} 
		catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void findPersonsByGender() {
		System.out.println("Select gender:");
		System.out.println("1 - male");
		System.out.println("2 - female");
		System.out.print("Select gender: ");
		
		Scanner sc = new Scanner(System.in);
		int genderId = sc.nextInt();
		sc.nextLine();
		
		String gender = "";
		if(genderId == 1) {
			gender = "male";
		}
		else if(genderId == 2){
			gender = "female";
		}
		else {
			System.out.println("Wrong input!");
			return;
		}
		
		String queryStr =             
				MessageFormat.format("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"+
						"PREFIX : <http://example.org/>\n"+
						"PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>\n"+
						"SELECT * WHERE '{' ?personid a :Person; :gender \"{0}\"@en '}'", gender);

		Query query = QueryFactory.create(queryStr);
		dataset.begin(ReadWrite.READ); // START TRANSACTION
		try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
			ResultSet results = qexec.execSelect() ;
			ResultSetFormatter.out(System.out, results, query) ;
		} 
		catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
		
	}

	private static void deletePerson() {
		System.out.println("\n\n------------------------------");
		System.out.print("Delete person: ");

		Scanner sc = new Scanner(System.in);
		String person = sc.nextLine();

		String deleteQuery = 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
						"PREFIX : <http://example.org/>" + 
						"DELETE {" + 
						":" + person + "?b ?c. }" +
						"INSERT { GRAPH :deleted {" +
						":" + person + " ?b ?c." +
						"}}"+
						"WHERE {" +
						":" + person + " ?b ?c." +
						"}";

		UpdateRequest request = UpdateFactory.create(deleteQuery);
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			UpdateAction.execute(request, dataset) ;
			dataset.commit();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void changePerson() {
		System.out.print("Change person: ");
		Scanner sc = new Scanner(System.in);
		String person = sc.nextLine();
		
		System.out.println("\n\n------------------------------");
		System.out.println("Actions:");
		System.out.println("1 - Change name");
		System.out.println("2 - Change address");
		System.out.println("3 - Change date of birth");
		System.out.println("4 - Change gender");
		System.out.println("5 - Change employer");
		
		System.out.print("Select action: ");
		int  action = sc.nextInt();
		sc.nextLine();
		
		switch (action) {
		case 1:
			changePersonName(person);
			break;
		case 2:
			changePersonAddress(person);
			break;
		case 3:
			changePersonDateOfBirth(person);
			break;
		case 4:
			changePersonGender(person);
			break;
		case 5:
			changePersonEmployer(person);
			break;
		default:
			System.out.println("Invalid input!");
			return;
		}
	}

	private static void changePersonEmployer(String person) {
		Scanner sc = new Scanner(System.in);
		System.out.print("Employer: ");
		String employer = sc.nextLine();
		
		String deleteQuery = 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
						"PREFIX : <http://example.org/>" + 
						"DELETE {" + 
						":" + person + " :employer ?e. }" +
						"INSERT {" +
						":" + person + " :employer \""+ employer+"\"." +
						"}"+
						"WHERE {" +
						":" + person + " :employer ?e." +
						"}";

		UpdateRequest request = UpdateFactory.create(deleteQuery);
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			UpdateAction.execute(request, dataset) ;
			dataset.commit();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void changePersonGender(String person) {
		Scanner sc = new Scanner(System.in);
		System.out.print("Gender: ");
		String gender = sc.nextLine();
		
		String deleteQuery = 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
						"PREFIX : <http://example.org/>" + 
						"DELETE {" + 
						":" + person + " :gender ?g. }" +
						"INSERT {" +
						":" + person + " :gender \""+ gender+"\"@en." +
						"}"+
						"WHERE {" +
						":" + person + " :gender ?g." +
						"}";

		UpdateRequest request = UpdateFactory.create(deleteQuery);
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			UpdateAction.execute(request, dataset) ;
			dataset.commit();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void changePersonDateOfBirth(String person) {
		Scanner sc = new Scanner(System.in);
		System.out.print("Date of birth: ");
		int birthdate = sc.nextInt();
		sc.nextLine();
		
		String deleteQuery = 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
						"PREFIX : <http://example.org/>" + 
						"DELETE {" + 
						":" + person + " :birthdate ?b. }" +
						"INSERT {" +
						":" + person + " :birthdate "+ birthdate + "." +
						"}"+
						"WHERE {" +
						":" + person + " :birthdate ?b." +
						"}";

		UpdateRequest request = UpdateFactory.create(deleteQuery);
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			UpdateAction.execute(request, dataset) ;
			dataset.commit();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void changePersonAddress(String person) {
		Scanner sc = new Scanner(System.in);
		System.out.print("Address: ");
		String address = sc.nextLine();
		
		String deleteQuery = 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
						"PREFIX : <http://example.org/>" + 
						"DELETE {" + 
						":" + person + " :address ?a. }" +
						"INSERT {" +
						":" + person + " :address \""+ address + "\"." +
						"}"+
						"WHERE {" +
						":" + person + " :address ?a." +
						"}";

		UpdateRequest request = UpdateFactory.create(deleteQuery);
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			UpdateAction.execute(request, dataset) ;
			dataset.commit();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void changePersonName(String person) {
		Scanner sc = new Scanner(System.in);
		System.out.print("Name: ");
		String name = sc.nextLine();
		
		String deleteQuery = 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
						"PREFIX : <http://example.org/>" + 
						"DELETE {" + 
						":" + person + " :name ?n. }" +
						"INSERT {" +
						":" + person + " :name \""+ name + "\"." +
						"}"+
						"WHERE {" +
						":" + person + " :name ?n." +
						"}";

		UpdateRequest request = UpdateFactory.create(deleteQuery);
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			UpdateAction.execute(request, dataset) ;
			dataset.commit();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void insertPerson() {
		System.out.println("\n\n------------------------------");
		Scanner sc = new Scanner(System.in);
		System.out.print("Insert person");
		System.out.print("Id: ");
		int id = sc.nextInt();
		sc.nextLine();
		System.out.print("Name: ");
		String name = sc.nextLine();
		System.out.print("Address: ");
		String address = sc.nextLine();
		System.out.print("Date of birth: ");
		int birthdate = sc.nextInt();
		sc.nextLine();
		System.out.print("Gender: ");
		String gender = sc.nextLine();
		System.out.print("Employer: ");
		String employer = sc.nextLine();
		
		String insertQuery = 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
						"PREFIX : <http://example.org/>" + 
						"INSERT DATA {" + 
						":p" + id + " a :Person;" +
						":name \""+ name+"\";"+
						":gender \""+ gender+"\"@en;"+
						":address \""+ address+"\";"+
						":birthdate "+ birthdate + ";" +
						":employer \""+ employer+"\";"+
						"}";

		UpdateRequest request = UpdateFactory.create(insertQuery);
		dataset.begin(ReadWrite.WRITE); // START TRANSACTION
		try {
			UpdateAction.execute(request, dataset) ;
			dataset.commit();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void showSpecific() {
		System.out.println("\n\n------------------------------");
		System.out.print("Show person: ");
		Scanner sc = new Scanner(System.in);
		String person = sc.nextLine();
		String endpoint = "http://query.wikidata.org/sparql";
		
		String queryStr =             
				MessageFormat.format("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"+
						"PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"+
						"PREFIX wd: <http://www.wikidata.org/entity/>\n"+
						"PREFIX wikibase: <http://wikiba.se/ontology#>\n"+
						"PREFIX bd: <http://www.bigdata.com/rdf#>\n"+
						"PREFIX : <http://example.org/>\n"+
						"SELECT (?n AS ?name) (?g AS ?gender) (?a as ?address) (?b as ?birthdate) (?e as ?employer) (?industryLabel as ?employerindustry) (?hqlocLabel as ?employerhq) (?countryLabel as ?employercountry)\n" +
						"WHERE '{':{0} :name ?n; :gender ?g; :address ?a; :birthdate ?b; :employer ?e.\n" +
						"OPTIONAL '{' GRAPH :Companies '{' ?c :name ?e; :wikidata ?w. '}' \n" +
						"SERVICE <http://query.wikidata.org/sparql> '{'SELECT DISTINCT ?w ?industryLabel ?hqlocLabel ?countryLabel\n"+
							"WHERE"+
							"'{'"+
							  	"?w wdt:P452 ?industry ;"+
							  	"wdt:P159 ?hqloc ;"+
							  	"wdt:P17 ?country ;"+
							  	"SERVICE wikibase:label '{' bd:serviceParam wikibase:language \"en\" '}'\n"+
						  "'}' LIMIT 1"+
						"'}}}'", person);
						
						/*String queryStr =             
				MessageFormat.format("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"+
						"PREFIX : <http://example.org/>\n"+
						"SELECT ?n ?g ?a ?b ?e WHERE '{':{0} :name ?n; :gender ?g; :address ?a; :birthdate ?b; :employer ?e '}'", person);*/


		
		Query query = QueryFactory.create(queryStr);
		dataset.begin(ReadWrite.READ); // START TRANSACTION
		try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
			ResultSet results = qexec.execSelect() ;
			ResultSetFormatter.out(System.out, results) ;
		} 
		catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}

	private static void showAll() {
		String queryStr =             
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"+
						"PREFIX : <http://example.org/>\n"+
						"SELECT (?a as ?personid) (?n as ?name) WHERE { ?a a :Person; :name ?n. }";

		Query query = QueryFactory.create(queryStr);
		dataset.begin(ReadWrite.READ); // START TRANSACTION
		try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
			ResultSet results = qexec.execSelect() ;
			ResultSetFormatter.out(System.out, results, query) ;
		} 
		catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}
	
	private static void showDeleted() {
		String queryStr =             
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"+
						"PREFIX : <http://example.org/>\n"+
						"SELECT * WHERE { GRAPH :deleted {?personid a :Person} }";

		Query query = QueryFactory.create(queryStr);
		dataset.begin(ReadWrite.READ); // START TRANSACTION
		try (QueryExecution qexec = QueryExecutionFactory.create(query, dataset)) {
			ResultSet results = qexec.execSelect() ;
			ResultSetFormatter.out(System.out, results) ;
		} 
		catch (RuntimeException e) {
			System.out.println(e.getMessage());
			dataset.abort(); //
		} finally { 
			dataset.end(); // END TRANSACTION (ABORT IF NO COMMIT)
		} 
	}
}
