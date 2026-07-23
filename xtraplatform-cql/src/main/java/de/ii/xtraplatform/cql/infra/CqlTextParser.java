/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.infra;

import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlParseException;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;

public class CqlTextParser {

  // Guard the recursive-descent parse (and the subsequent tree visit) against deeply nested or
  // oversized input: both recurse with the nesting depth of the expression, so a filter with a few
  // thousand nested parentheses would otherwise overflow the stack and crash the request thread.
  private static final int MAX_LENGTH = 100_000;
  private static final int MAX_NESTING_DEPTH = 250;

  private CqlParser.CqlFilterContext parseToTree(String cql) {
    CqlLexer lexer = new CqlLexer(CharStreams.fromString(cql));
    lexer.removeErrorListeners();
    lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

    CommonTokenStream tokens = new CommonTokenStream(lexer);

    CqlParser parser = new CqlParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(ThrowingErrorListener.INSTANCE);

    return parser.cqlFilter();
  }

  @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
  public Cql2Expression parse(String cql, EpsgCrs defaultCrs) throws CqlParseException {
    return parse(cql, new CqlTextVisitor(defaultCrs));
  }

  @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
  public Cql2Expression parse(String cql, CqlTextVisitor visitor) throws CqlParseException {
    checkComplexity(cql);
    try {
      CqlParser.CqlFilterContext cqlFilterContext = parseToTree(cql);

      return (Cql2Expression) visitor.visit(cqlFilterContext);
    } catch (ParseCancellationException e) {
      throw new CqlParseException(e.getMessage(), e);
    } catch (StackOverflowError e) {
      // Safety net in case a construct recurses past the stack despite the depth guard.
      throw new CqlParseException("the filter expression is nested too deeply");
    }
  }

  // Reject an over-long or too-deeply-nested filter before parsing. Nesting depth is measured by
  // counting parentheses outside string literals (single-quoted, with '' as an escaped quote), so
  // parentheses inside a literal value do not count toward the limit.
  private static void checkComplexity(String cql) throws CqlParseException {
    if (cql == null) {
      return;
    }
    if (cql.length() > MAX_LENGTH) {
      throw new CqlParseException(
          "the filter expression exceeds the maximum length of " + MAX_LENGTH + " characters");
    }
    int depth = 0;
    int maxDepth = 0;
    boolean inString = false;
    for (int i = 0; i < cql.length(); i++) {
      char c = cql.charAt(i);
      if (c == '\'') {
        if (inString && i + 1 < cql.length() && cql.charAt(i + 1) == '\'') {
          i++; // skip an escaped '' quote, staying inside the string
        } else {
          inString = !inString;
        }
      } else if (!inString) {
        if (c == '(') {
          depth++;
          if (depth > maxDepth) {
            maxDepth = depth;
          }
        } else if (c == ')' && depth > 0) {
          depth--;
        }
      }
    }
    if (maxDepth > MAX_NESTING_DEPTH) {
      throw new CqlParseException(
          "the filter expression is nested too deeply (maximum "
              + MAX_NESTING_DEPTH
              + " levels of parentheses)");
    }
  }

  public static class ThrowingErrorListener extends BaseErrorListener {

    public static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

    @Override
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
    public void syntaxError(
        Recognizer<?, ?> recognizer,
        Object offendingSymbol,
        int line,
        int charPositionInLine,
        String msg,
        RecognitionException e)
        throws ParseCancellationException {
      throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg);
    }
  }

  public abstract static class CqlParserCustom extends Parser {

    public CqlParserCustom(TokenStream input) {
      super(input);
    }

    protected final Boolean isNotInsideNestedFilter(ParserRuleContext ctx) {
      ParserRuleContext current = ctx;

      while (current.parent != null) {
        current = (ParserRuleContext) current.parent;

        if (current instanceof CqlParser.NestedCqlFilterContext) {
          return false;
        }
      }

      return true;
    }
  }
}
