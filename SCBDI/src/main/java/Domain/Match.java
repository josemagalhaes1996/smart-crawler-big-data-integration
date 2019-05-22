/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Domain;

import java.util.List;

/**
 *
 * @author Utilizador
 */
public class Match {
    private Token newColumn; 
    private Token columnBDW;
    private double score;

    public Match(Token newColumn, Token columnBDW, double score) {
        this.newColumn = newColumn;
        this.columnBDW = columnBDW;
        this.score = score;
    }

    public Match() {
    }

    public Token getNewColumn() {
        return newColumn;
    }

    public void setNewColumn(Token newColumn) {
        this.newColumn = newColumn;
    }

    public Token getColumnBDW() {
        return columnBDW;
    }

    public void setColumnBDW(Token columnBDW) {
        this.columnBDW = columnBDW;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
    
    
    
    
}
