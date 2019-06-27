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

    private double jaccardTime;
    private double jaro_winklerTime;
    private double levensheteinTime;
    private double cosineTime;

    private double JiangandConrath;
    private double Wu_Palmer;
    private double PATH;

    private double hashMatcher; 
    private double hashTime;
    
    private int constructor;

    private double averageSimilarity;
    private double averageTime;

    public Score(double jaccard, double jaro_winkler, double levenshetein, double cosine) {
        this.constructor = 1;
        this.jaccard = jaccard;
        this.jaro_winkler = jaro_winkler;
        this.levenshetein = levenshetein;
        this.cosine = cosine;
        this.averageSimilarity = (cosine + jaccard + jaro_winkler + levenshetein) / 4;
    }

    public Score(double JiangandConrath, double Wu_Palmer, double PATH) {
        this.constructor = 2;
        this.JiangandConrath = JiangandConrath;
        this.Wu_Palmer = Wu_Palmer;
        this.PATH = PATH;
        this.averageSimilarity = (JiangandConrath + Wu_Palmer + PATH) / 3;
    }

    public Score(double jaccard, double jaccardTime, double jaro_winkler, double jaro_winklerTime, double levenshtein, double levenshteinTime, double cosine, double cosineTime) {
        this.constructor = 3;
        this.jaccard = jaccard;
        this.jaro_winkler = jaro_winkler;
        this.levenshetein = levenshtein;
        this.cosine = cosine;

        this.jaccardTime = jaccardTime;
        this.cosineTime = cosineTime;
        this.jaro_winklerTime = jaro_winklerTime;
        this.levensheteinTime = levenshteinTime;

        this.averageSimilarity = (cosine + jaccard + jaro_winkler + levenshtein) / 4;
        this.averageTime = (jaccardTime + cosineTime + jaro_winklerTime + levenshteinTime) / 4;

    }

    public Score(double jaccard, double jaccardTime, double jaro_winkler, double jaro_winklerTime, double levenshtein, double levenshteinTime, double cosine, double cosineTime, double hashSimilarity, double hashTime) {
        this.constructor = 3;
        this.jaccard = jaccard;
        this.jaro_winkler = jaro_winkler;
        this.levenshetein = levenshtein;
        this.cosine = cosine;

        this.jaccardTime = jaccardTime;
        this.cosineTime = cosineTime;
        this.jaro_winklerTime = jaro_winklerTime;
        this.levensheteinTime = levenshteinTime;
        
        this.hashMatcher = hashSimilarity;
        this.hashTime = hashTime;
        
        this.averageSimilarity = (cosine + jaccard + jaro_winkler + levenshtein) / 4;
        this.averageTime = (jaccardTime + cosineTime + jaro_winklerTime + levenshteinTime) / 4;

    }

    public double getHashMatcher() {
        return hashMatcher;
    }

    public void setHashMatcher(double hashMatcher) {
        this.hashMatcher = hashMatcher;
    }

    public double getHashTime() {
        return hashTime;
    }

    public void setHashTime(double hashTime) {
        this.hashTime = hashTime;
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

    public double getJaccardTime() {
        return jaccardTime;
    }

    public void setJaccardTime(long jaccardTime) {
        this.jaccardTime = jaccardTime;
    }

    public double getJaro_winklerTime() {
        return jaro_winklerTime;
    }

    public void setJaro_winklerTime(long jaro_winklerTime) {
        this.jaro_winklerTime = jaro_winklerTime;
    }

    public double getLevensheteinTime() {
        return levensheteinTime;
    }

    public void setLevensheteinTime(long levensheteinTime) {
        this.levensheteinTime = levensheteinTime;
    }

    public double getCosineTime() {
        return cosineTime;
    }

    public void setCosineTime(long cosineTime) {
        this.cosineTime = cosineTime;
    }

    public double getJiangandConrath() {
        return JiangandConrath;
    }

    public void setJiangandConrath(double JiangandConrath) {
        this.JiangandConrath = JiangandConrath;
    }

    public double getWu_Palmer() {
        return Wu_Palmer;
    }

    public void setWu_Palmer(double Wu_Palmer) {
        this.Wu_Palmer = Wu_Palmer;
    }

    public double getPATH() {
        return PATH;
    }

    public void setPATH(double PATH) {
        this.PATH = PATH;
    }

    public int getConstructor() {
        return constructor;
    }

    public void setConstructor(int constructor) {
        this.constructor = constructor;
    }

    public double getAverageSimilarity() {
        return averageSimilarity;
    }

    public void setAverageSimilarity(double averageSimilarity) {
        this.averageSimilarity = averageSimilarity;
    }

    public double getAverageTime() {
        return averageTime;
    }

    public void setAverageTime(double averageTime) {
        this.averageTime = averageTime;
    }

}
