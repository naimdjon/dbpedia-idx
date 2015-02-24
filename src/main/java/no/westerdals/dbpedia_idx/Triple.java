package no.westerdals.dbpedia_idx;

public final class Triple {

    public final String subject, predicate, property;

    public Triple(String subject, String predicate, String property) {
        this.subject = subject;
        this.predicate = predicate;
        this.property = property;
    }

    @Override
    public String toString() {
        return "Triple{" +
                "subject='" + subject + '\'' +
                ", predicate='" + predicate + '\'' +
                ", property='" + property + '\'' +
                '}';
    }
}