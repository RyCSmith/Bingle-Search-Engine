

import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.document.Item;

@DynamoDBTable(tableName = "index")
public class InvertedIndexEntry extends Item{

	private String word;
	private String docReferences;
	
	public InvertedIndexEntry(String line){
		String[] parts = line.split("\t");
		word = parts[0].trim();
		docReferences = parts[1].trim();
	}

	@DynamoDBHashKey
	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	@DynamoDBAttribute
	public String getDocReferences() {
		return docReferences;
	}

	public void setDocReferences(String docReferences) {
		this.docReferences = docReferences;
	}
	
//	public String[] getAllTermDocs(){
//		return docReferences.split("|");
//	}

}
