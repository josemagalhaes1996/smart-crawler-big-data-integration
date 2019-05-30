/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Domain;

/**
 *
 * @author Utilizador
 */
public class Score {

    private double jaccard;
    private double jaro_winkler;
    private double levenshetein;
    private double cosine;
    private double averageAll;

    public Score(double jaccard, double jaro_winkler, double levenshetein, double cosine) {
        this.jaccard = jaccard;
        this.jaro_winkler = jaro_winkler;
        this.levenshetein = levenshetein;
        this.cosine = cosine;
        this.averageAll = (cosine + jaccard + jaro_winkler + levenshetein) / 4;
    }

    public double getJaccard() {
        return jaccard;
    }

    public void setJaccard(double jaccard) {
        this.jaccard = jaccard;
    }

    public double getJaro_winkler() {
        return jaro_winkler;
    }

    public void setJaro_winkler(double jaro_winkler) {
        this.jaro_winkler = jaro_winkler;
    }

    public double getLevenshetein() {
        return levenshetein;
    }

    public void setLevenshetein(double levenshetein) {
        this.levenshetein = levenshetein;
    }

    public double getCosine() {
        return cosine;
    }

    public void setCosine(double cosine) {
        this.cosine = cosine;
    }

    public double getAverageAll() {
        return averageAll;
    }

    public void setAverageAll(double averageAll) {
        this.averageAll = averageAll;
    }

}
