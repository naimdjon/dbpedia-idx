package no.westerdals.dbpedia_idx.index;

public class DocumentInformation {
    final String md5id;
    final int documentNumber;

    public DocumentInformation(String md5id, int documentNumber) {
        this.md5id = md5id;
        this.documentNumber = documentNumber;
    }
}
