package niagara.xmlql_parser;

/** Abstract interface for lexers */
public interface Lexer {
	public java_cup.runtime.Symbol next_token() throws java.io.IOException;
}
