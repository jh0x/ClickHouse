#pragma once

#include <Core/Field.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/TokenIterator.h>
#include <Common/FieldVisitorDump.h>
#include <DataTypes/IDataType.h>

#include <optional>


namespace DB
{

/// Literal (atomic) - number, string, NULL
class ASTLiteral : public ASTWithAlias
{
public:
    explicit ASTLiteral(Field value_) : value(std::move(value_)) {}

    // This methond and the custom_type are only used for Apache Gluten,
    explicit ASTLiteral(Field value_, DataTypePtr & type_) : value(std::move(value_))
    {
        custom_type = type_;
    }

    Field value;
    DataTypePtr custom_type;

    /// For ConstantExpressionTemplate
    std::optional<TokenIterator> begin;
    std::optional<TokenIterator> end;

    /*
     * The name of the column corresponding to this literal. Only used to
     * disambiguate the literal columns with the same display name that are
     * created at the expression analyzer stage. In the future, we might want to
     * have a full separation between display names and column identifiers. For
     * now, this field is effectively just some private EA data.
     */
    String unique_column_name;

    /// For compatibility reasons in distributed queries,
    /// we may need to use legacy column name for tuple literal.
    bool use_legacy_column_name_of_tuple = false;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "Literal" + (delim + applyVisitor(FieldVisitorDump(), value)); }

    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    /// Legacy version of 'appendColumnNameImpl'. It differs only with tuple literals.
    /// It's only needed to continue working of queries with tuple literals
    /// in distributed tables while rolling update.
    void appendColumnNameImplLegacy(WriteBuffer & ostr) const;
};


/** A special UX improvement. If it's [I]LIKE or NOT [I]LIKE expression and the right hand side is a string literal,
  *  we will highlight unescaped metacharacters % and _ in string literal for convenience.
  * Motivation: most people are unaware that _ is a metacharacter and forgot to properly escape it with two backslashes.
  * With highlighting we make it clearly obvious.
  *
  * Another case is regexp match. Suppose the user types match(URL, 'www.clickhouse.com'). It often means that the user is unaware that . is a metacharacter.
  */
bool highlightStringLiteralWithMetacharacters(const ASTPtr & node, WriteBuffer & ostr, const char * metacharacters);
void highlightStringWithMetacharacters(const String & string, WriteBuffer & ostr, const char * metacharacters);

}
