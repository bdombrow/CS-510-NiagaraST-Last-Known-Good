package niagara.xmlql_parser.syntax_tree;

import java_cup.runtime.*;

%%

%class Scanner
%implements Lexer
%caseless
%public

%pack

%line
%column

%cup

%{
  StringBuffer string = new StringBuffer();
  String s = new String();
  private Symbol symbol(int type) {
	return new Symbol(type, yyline, yycolumn);
  	}

  private Symbol symbol(int type, Object value) {
	return new Symbol(type, yyline, yycolumn, value);
	}
%}

Space = [ \t]+

LineEnd = [\n|\r]

Comment = "/*" [^*] {CommentContent} \*+ "/"
 
CommentContent = ([^*] | \*+[^*/])*

Digits = ([0-9])+
Number = ("+"|"-")?{Digits}("."{Digits})?("E"("+"|"-")?{Digits})?

Var = "$"{Identifier}

Identifier = [a-zA-Z0-9]([:jletterdigit:])*

StringChar = [^\"\n]

%state STRING

%%

<YYINITIAL> {
/* keywords */

  "in" 		{ return symbol(sym.IN); }
  "where" 	{ return symbol(sym.WHERE); }
  "construct" 	{ return symbol(sym.CONSTRUCT); }
  "element_as" 	{ return symbol(sym.ELEMENT_AS); }
  "content_as"  { return symbol(sym.CONTENT_AS); }
  "ordered_by"  { return symbol(sym.ORDERED_BY); }
  "conform_to"  { return symbol(sym.CONFORM_TO); }
  
  "function" 	{ return symbol(sym.FUNCTION); }
  "end" 	{ return symbol(sym.END); }
  
  "create"      { return symbol(sym.CREATE); }
  "delete"	{ return symbol(sym.DELETE); }
  "trigger"     { return symbol(sym.TRIGGER); }
  "once"	{ return symbol(sym.ONCE); }
  "multiple"    { return symbol(sym.MULTIPLE); }
  "start"	{ return symbol(sym.START); }
  "every"	{ return symbol(sym.EVERY); }
  "expire"	{ return symbol(sym.EXPIRE); }
  "do"		{ return symbol(sym.DO); }

  "or"		{ return symbol(sym.OR); }
  "and"		{ return symbol(sym.AND); }
  "not"		{ return symbol(sym.NOT); }

  "(" 		{ return symbol(sym.LPAREN); }
  ")" 		{ return symbol(sym.RPAREN); }
  "{" 		{ return symbol(sym.LBRACE); }
  "}" 		{ return symbol(sym.RBRACE); }
  "/" 		{ return symbol(sym.SLASH); }
  "," 		{ return symbol(sym.COMMA); }
  "." 		{ return symbol(sym.DOT); }
  "?" 		{ return symbol(sym.QMARK); }
  "+" 		{ return symbol(sym.PLUS); }
  "*"		{ return symbol(sym.STAR); }
  ":"		{ return symbol(sym.COLON); }
  "|" 		{ return symbol(sym.BAR); }
  "$"		{ return symbol(sym.DOLLAR); }
  
  "<" 		{ return symbol(sym.LT); }
  ">" 		{ return symbol(sym.GT); }
  "=" 		{ return symbol(sym.EQ); }
  "<="		{ return symbol(sym.LEQ); }
  ">="		{ return symbol(sym.GEQ); }
  "!="		{ return symbol(sym.NEQ); }

  \" 		{ yybegin(STRING); s = new String();}

  "id"		{ return symbol(sym.ID); }
  {Identifier} 	{ return symbol(sym.IDEN, new String(yytext())); }
  {Var} 	{ return symbol(sym.VAR, new String(yytext())); }
  {Number}      { return symbol(sym.NUMBER,new String(yytext())); }

  {Comment} 	{ /* ignore */ }
}

<STRING> {
  \" 			{ yybegin(YYINITIAL); 
     		  	  return symbol(sym.STRING, s); 
   			}
  {StringChar}+ 	{ s += yytext(); }
}

.|{Space}|{LineEnd} 	{ /* ignore */ }




