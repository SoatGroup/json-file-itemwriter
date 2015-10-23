## JsonflatItemWriter
Implémentation l'item writer de Spring batch permettant de produire un fichier json valide.

Les modules se décomposent comme suit: 

* jsonitem-writer-impl-objectif1: configuration de FlatFileItemWriter pour la production d'un fichier texte.
* jsonitem-writer-impl-objectif2: configuration de FlatFileItemWriter pour la production d'un fichier json.
* jsonitem-writer-api : Api permettant suite à configuration spring batch, de produire du json.
* jsonitem-writer-impl: Configuration se basant sur l'api jsonitem-writer-api pour produire un fichier json valide.
* jsonitem-writer-common : classes utiles à tout le projet.

###Configuration permettant de produire du json 
* Etape 1 : Ajouter l'api en dépendance du projet

    
            <dependency>
        			<groupId>fr.soat.java</groupId>
        			<artifactId>jsonitem-writer-api</artifactId>
        			<version>1.0-SNAPSHOT</version>
        	</dependency>
        	
* Etape 2 : configuration du writer - exemple par annotation: 

** Noeud racine sans clef. Exemple de sortie attendue:
        


            [
                {"clef1": "valeur1"},
                {"clef2": "valeur2"},
                {"clef2": "valeur2"}
                ]
            
Instancier le constructeur par défaut.
      
      
      
      JsonFlatFileItemWriter<Person> writer = new JsonFlatFileItemWriter<>();


** Noeud racine avec clef - Exemple de sortie attendue:
    
    {"persons" : [
        {"clef1": "valeur1"},
        {"clef2": "valeur2"},
        {"clef3": "valeur3"}
    ]}

Instancier le constructeur par défaut en lui passant la valeur de la clef en paramètre

    JsonFlatFileItemWriter<Person> writer = new JsonFlatFileItemWriter<Person>(persons);

* Etape 3 : Renseigner l'instance de  JsonItemAggregator (conversion du pojo en Strng).
 
        writer.setJsonItemAggregator(new JsonItemAggregator<Person>());    
	
* Etape 4 : Définition des options de fichier de sortie 

    	writer.setResource(new FileSystemResource (...)); 
    	writer.setEncoding(AppUtils.UTF_8.name()); //facultatif
    	writer.setShouldDeleteIfExists(true); //facultatif
	
* Etape 5 : Exécutez! c'est fini. 

