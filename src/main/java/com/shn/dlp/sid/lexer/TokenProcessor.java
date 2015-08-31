package com.shn.dlp.sid.lexer;

import org.antlr.v4.runtime.Token;

public interface TokenProcessor {

	public void process(Token token, String text, String type);
}
