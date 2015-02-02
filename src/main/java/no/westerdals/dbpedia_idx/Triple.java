package no.westerdals.dbpedia_idx;

public final class Triple {

    public final String subject, predicate, object;

    public Triple(String subject, String predicate, String object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }
}

