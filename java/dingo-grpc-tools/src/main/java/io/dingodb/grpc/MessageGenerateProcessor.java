package io.dingodb.grpc;


import com.google.common.base.CaseFormat;
import com.google.protobuf.ByteString;
import com.google.protobuf.MapField;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Delegate;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.squareup.javapoet.TypeName.INT;
import static com.squareup.javapoet.TypeName.VOID;
import static io.dingodb.grpc.Constant.CASE_END;
import static io.dingodb.grpc.Constant.FIELD_END;
import static io.dingodb.grpc.Constant.FIELD_NUMBER_END;
import static io.dingodb.grpc.Constant.INPUT;
import static io.dingodb.grpc.Constant.INPUT_ARG;
import static io.dingodb.grpc.Constant.MESSAGE;
import static io.dingodb.grpc.Constant.MSG_PKG;
import static io.dingodb.grpc.Constant.NUMBER;
import static io.dingodb.grpc.Constant.NUMERIC;
import static io.dingodb.grpc.Constant.OUT;
import static io.dingodb.grpc.Constant.OUT_ARG;
import static io.dingodb.grpc.Constant.READER;
import static io.dingodb.grpc.Constant.SIZE_OF;
import static io.dingodb.grpc.Constant.SIZE_UTILS;
import static io.dingodb.grpc.Constant.VALUE;
import static io.dingodb.grpc.Constant.WRITE;
import static io.dingodb.grpc.Constant.WRITER;
import static io.dingodb.grpc.SerializeGenerateProcessor.canDirect;
import static io.dingodb.grpc.SerializeGenerateProcessor.directRead;
import static io.dingodb.grpc.SerializeGenerateProcessor.readStatement;
import static io.dingodb.grpc.SerializeGenerateProcessor.sizeOfStatement;
import static io.dingodb.grpc.SerializeGenerateProcessor.writeStatement;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

public class MessageGenerateProcessor {

    private final Types types;
    private final Elements elements;

    protected final Map<ClassName, TypeSpec.Builder> messages = new HashMap<>();
    private final Map<ClassName, TypeSpec.Builder> enumMessages = new HashMap<>();

    public MessageGenerateProcessor(Types types, Elements elements) {
        this.types = types;
        this.elements = elements;
    }

    public String packageOf(Element element) {
        return elements.getPackageOf(types.asElement(element.asType())).getSimpleName().toString();
    }

    public boolean exist(ClassName className) {
        return messages.containsKey(className) || elements.getTypeElement(className.canonicalName()) != null;
    }

    public ClassName generateMessage(TypeElement element) {
        return generateMessage(null, element);
    }

    private TypeName fieldType(String name, Element element, Map<String, ? extends Element> parentElements) {
        if (element.asType().getKind().isPrimitive()) {
            TypeMirror primitiveType = element.asType();
            if (primitiveType.getKind() == TypeKind.INT) {
                element = this.types.asElement(((ExecutableElement) parentElements.get(
                    UPPER_UNDERSCORE.to(LOWER_CAMEL, "GET_" + LOWER_CAMEL.to(UPPER_UNDERSCORE, name) + "()")
                )).getReturnType());
                if (element != null && element.getKind() == ElementKind.ENUM) {
                    ClassName className = ClassName.get(
                        MSG_PKG + "." + packageOf(element), element.getSimpleName().toString());
                    generateEnum(className, (TypeElement) element);
                    return className;
                }
            }
            return ClassName.get(primitiveType);
        }
        if (element.asType().toString().equals(Object.class.getName())) {
            element = this.types.asElement(((ExecutableElement) parentElements.get(
                UPPER_UNDERSCORE.to(LOWER_CAMEL, "GET_" + LOWER_CAMEL.to(UPPER_UNDERSCORE, name) + "()")
            )).getReturnType());
            return fieldType(name, element, parentElements);
        }

        if (ByteString.class.getName().equals(element.asType().toString())) {
            return ArrayTypeName.of(byte.class);
        }

        if (Object.class.getPackage().getName().equals(elements.getPackageOf(element).toString())) {
            return ClassName.get(element.asType());
        }

        if (types.isAssignable(
            types.erasure(element.asType()), types.erasure(elements.getTypeElement(List.class.getName()).asType())
        )) {

            ClassName className = ClassName.get(List.class);
            DeclaredType declaredType = (DeclaredType) element.asType();
            while (declaredType.getTypeArguments().size() < 1) {
                declaredType = (DeclaredType) types.directSupertypes(declaredType).get(1);
            }
            TypeName arg = fieldType(null, types.asElement(declaredType.getTypeArguments().get(0)), parentElements);
            return ParameterizedTypeName.get(className, arg);
        }

        if (types.isAssignable(
            types.erasure(element.asType()), types.erasure(elements.getTypeElement(Map.class.getName()).asType())
        ) || types.isAssignable(
            types.erasure(element.asType()), types.erasure(elements.getTypeElement(MapField.class.getName()).asType())
        )) {

            ClassName className = ClassName.get(Map.class);
            DeclaredType declaredType = (DeclaredType) element.asType();
            while (declaredType.getTypeArguments().size() < 2) {
                declaredType = (DeclaredType) types.directSupertypes(declaredType).get(1);
            }
            TypeName arg1 = fieldType(null, types.asElement(declaredType.getTypeArguments().get(0)), parentElements);
            TypeName arg2 = fieldType(null, types.asElement(declaredType.getTypeArguments().get(1)), parentElements);
            return ParameterizedTypeName.get(className, arg1, arg2);
        }

        element = types.asElement(element.asType());
        ClassName className = ClassName.get(MSG_PKG + "." + packageOf(element), element.getSimpleName().toString());
        generateMessage(className, (TypeElement) element);
        return className;
    }

    private CodeBlock makeWriteStatement(String fieldName, TypeName fieldType, int number) {
        if (number == 0) {
            return CodeBlock.of("$T.$L($L, $L, $L)", WRITER, WRITE, fieldName, fieldName, OUT);
        } else {
            return writeStatement(fieldName, fieldType, number);
        }
    }

    private CodeBlock makeSizeOfStatement(String fieldName, TypeName fieldType, int number) {
        if (number == 0) {
            return CodeBlock.of("$T.$L($L, $L)", SIZE_UTILS, SIZE_OF, fieldName, fieldName);
        } else {
            return sizeOfStatement(fieldName, fieldType, number);
        }
    }

    private CodeBlock makeReadStatement(String fieldName, TypeName fieldType, int number) {
        CodeBlock readStatement;
        if (enumMessages.containsKey(fieldType)) {
            readStatement = CodeBlock.of(
                "case $L: $L = ($T.forNumber($T.readInt($L)))",
                number, fieldName, fieldType, READER, INPUT
            );
        } else {
            readStatement = readStatement(fieldName, fieldType, number);
        }
        return readStatement;
    }

    private void generateEnum(ClassName className, TypeElement element) {
        TypeSpec.Builder builder = TypeSpec.enumBuilder(className).addModifiers(PUBLIC)
            .addSuperinterface(NUMERIC)
            .addField(Integer.class, NUMBER, PUBLIC, FINAL)
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(PRIVATE)
                .addParameter(Integer.class, NUMBER)
                .addStatement("this.$L = $L", NUMBER, NUMBER)
                .build())
            .addMethod(overrideBuilder(NUMBER, INT)
                .addStatement("return $L", NUMBER)
                .build()
            );
        Map<String, Element> elements = element
            .getEnclosedElements().stream()
            .collect(toMap(Object::toString, __ -> __));
        MethodSpec.Builder forNumberMethod = methodBuilder("forNumber", className, PUBLIC, STATIC)
            .addParameter(INT, "number");
        forNumberMethod.beginControlFlow("switch(number)");
        NavigableMap<Integer, String> sortedEnum = new TreeMap<>();
        for (String name : elements.keySet()) {
            if (!(elements.get(name).getKind() == ElementKind.ENUM_CONSTANT)) {
                continue;
            }
            VariableElement variableElement = (VariableElement) elements.get(name + "_VALUE");
            Integer number = variableElement == null ? -1 : (Integer) variableElement.getConstantValue();
            sortedEnum.put(number, name);
        }
        for (Map.Entry<Integer, String> entry : sortedEnum.entrySet()) {
            Integer number = entry.getKey();
            String name = entry.getValue();
            builder.addEnumConstant(name, TypeSpec.anonymousClassBuilder("$L", number).build());
            forNumberMethod.addStatement("case $L: return $L", number, name);
        }
        forNumberMethod.addStatement("default: return null");
        forNumberMethod.endControlFlow();
        builder.addMethod(forNumberMethod.build());
        messages.put(className, builder);
        enumMessages.put(className, builder);
    }

    private ClassName generateMessage(ClassName className, TypeElement element) {

        if (className != null && exist(className)) {
            return className;
        }

        ClassName newClassName = className;
        if (newClassName == null) {
            newClassName = ClassName.get(MSG_PKG + "." + packageOf(element), element.getSimpleName().toString());
        }

        TypeSpec.Builder builder = TypeSpec.classBuilder(newClassName).addModifiers(PUBLIC).addSuperinterface(MESSAGE)
            .addAnnotation(Data.class).addAnnotation(Builder.class).addAnnotation(NoArgsConstructor.class);

        Map<String, Integer> real = new HashMap<>();
        NavigableMap<Integer, Map.Entry<String, TypeName>> all = new TreeMap<>();

        Map<String, Element> elements = element.getEnclosedElements().stream().collect(toMap(Object::toString, identity()));
        for (String name : new ArrayList<>(elements.keySet())) {
            if (name.endsWith(FIELD_NUMBER_END)) {
                String numberName = UPPER_UNDERSCORE.to(
                    LOWER_CAMEL, name.substring(0, name.length() - FIELD_NUMBER_END.length())
                ).toUpperCase();
                elements.put(numberName, elements.get(name));
            }
        }
        for (String name : elements.keySet()) {
            if (!name.endsWith(FIELD_END)) {
                continue;
            }
            if (name.endsWith("converter_")
                && elements.containsKey(name.substring(0, name.length() - "converter_".length()))
            ) {
                continue;
            }
            if (name.endsWith(CASE_END)
                && elements.containsKey(name.substring(0, name.length() - CASE_END.length()) + "_")
            ) {
                continue;
            }
            String fieldName = name.substring(0, name.length() - 1);

            Element fieldElement = elements.get(name);
            VariableElement numberField = (VariableElement) elements.get(fieldName.toUpperCase());
            int number = 0;

            TypeName fieldType;
            if (numberField == null) {
                // one of message
                TypeElement oneOfEnum = this.elements.getTypeElement(
                    element.getQualifiedName().toString() + "."
                        + LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, fieldName + "Case")
                );
                fieldType = newClassName.nestedClass(LOWER_CAMEL.to(UPPER_CAMEL, fieldName + "Nest"));
                TypeSpec oneOfType = generateOneOfMessage(fieldName, (ClassName) fieldType, oneOfEnum, elements, all);
                builder.addType(oneOfType);
            } else {
                number = (Integer) numberField.getConstantValue();
                fieldType = fieldType(fieldName, fieldElement, elements).box();
            }
            if (number != 0) {
                all.put(number, new AbstractMap.SimpleEntry<>(fieldName, fieldType));
            }
            real.put(fieldName, number);

            builder.addField(FieldSpec.builder(fieldType, fieldName, PRIVATE).build());

        }

        if (builder.fieldSpecs.size() > 0) {
            builder.addAnnotation(AllArgsConstructor.class);
        }

        makeReadWriteMethod(builder, real, all);
        messages.put(newClassName, builder);

        return newClassName;
    }

    private TypeSpec generateOneOfMessage(
        String fieldName,
        ClassName className,
        TypeElement element,
        Map<String, ? extends Element> parentElements,
        NavigableMap<Integer, Map.Entry<String, TypeName>> all
    ) {

        List<? extends Element> oneOfElement = element.getEnclosedElements().stream()
            .filter(__ -> __.getKind() == ElementKind.ENUM_CONSTANT)
            .collect(Collectors.toList());

        ClassName nest = className.nestedClass("Nest");
        TypeSpec.Builder builder = TypeSpec
            .interfaceBuilder(className)
            .addModifiers(PUBLIC)
            .addSuperinterface(MESSAGE).addSuperinterface(NUMERIC)
            .addMethod(methodBuilder(NUMBER, INT, PUBLIC, ABSTRACT).build())
            .addMethod(methodBuilder("nest", nest, PUBLIC, ABSTRACT).build());

        TypeSpec.Builder nestBuilder = TypeSpec.enumBuilder(nest).addModifiers(PUBLIC, STATIC)
            .addSuperinterface(NUMERIC)
            .addField(Integer.class, NUMBER, PUBLIC, FINAL)
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(PRIVATE)
                .addParameter(Integer.class, NUMBER)
                .addStatement("this.$L = $L", NUMBER, NUMBER)
                .build())
            .addMethod(overrideBuilder(NUMBER, INT)
                .addStatement("return $L", NUMBER)
                .build()
            );

        for (Element enumElement : oneOfElement) {
            String name = enumElement.toString();
            Integer number = ofNullable(parentElements.get(name + FIELD_NUMBER_END))
                .map(__ -> ((VariableElement) __).getConstantValue()).map(Integer.class::cast).orElse(null);
            if (name.endsWith("_NOT_SET") && number == null) {
                continue;
            }

            ClassName nestClassName = className.nestedClass(UPPER_UNDERSCORE.to(UPPER_CAMEL, name));
            TypeSpec.Builder nestClassBuilder = TypeSpec.classBuilder(nestClassName);

            TypeMirror returnType = ((ExecutableElement) parentElements.get(
                UPPER_UNDERSCORE.to(LOWER_CAMEL, "GET_" + name) + "()")
            ).getReturnType();
            TypeName realTypeName;
            FieldSpec.Builder realValueField;
            if (returnType.getKind().isPrimitive()) {
                realTypeName = ClassName.get(returnType).box();
                realValueField = FieldSpec.builder(realTypeName, VALUE, PRIVATE);
            } else {
                TypeElement realType = (TypeElement) types.asElement(returnType);
                realTypeName = fieldType(null, realType, parentElements);
                realValueField = FieldSpec.builder(realTypeName, VALUE, PRIVATE);
                if (realTypeName instanceof ClassName) {
                    generateMessage((ClassName) realTypeName, realType);
                    realValueField.addAnnotation(Delegate.class);
                }
            }

            if (canDirect(realTypeName)) {
                nestClassBuilder.addMethod(overrideBuilder("read", VOID, INPUT_ARG)
                    .addStatement("$L = $T.$L($L)", VALUE, READER, directRead(realTypeName), INPUT)
                    .build()
                ).addMethod(overrideBuilder("write", VOID, OUT_ARG)
                    .addStatement("$T.$L($L, $L)", WRITER, WRITE, VALUE, OUT)
                    .build()
                ).addMethod(overrideBuilder(SIZE_OF, INT)
                    .addStatement("return $T.$L($L)", SIZE_UTILS, SIZE_OF, VALUE)
                    .build()
                );
            }

            all.put(number, new AbstractMap.SimpleEntry<>(fieldName, nestClassName));

            if (!canDirect(realTypeName)) {
                realValueField.initializer("new $T()", realTypeName);
            }

            nestBuilder.addEnumConstant(name, TypeSpec.anonymousClassBuilder("$L", number).build());

            builder.addType(
                nestClassBuilder
                    .addAnnotation(Data.class)
                    .addSuperinterface(className)
                    .addModifiers(PUBLIC, STATIC)
                    .addField(realValueField.build())
                    .addField(FieldSpec.builder(INT, NUMBER, PUBLIC, STATIC, FINAL).initializer("$L", number).build())
                    .addMethod(overrideBuilder(NUMBER, INT).addStatement("return $L", number).build())
                    .addMethod(overrideBuilder("nest", nest).addStatement("return $T.$L", nest, name).build())
                    .build()
            );
        }

        return builder.addType(nestBuilder.build()).build();
    }

    private void makeReadWriteMethod(
        TypeSpec.Builder builder, Map<String, Integer> real, NavigableMap<Integer, Map.Entry<String, TypeName>> all
    ) {

        MethodSpec.Builder writeBuilder = overrideBuilder(WRITE, VOID, OUT_ARG);
        MethodSpec.Builder sizeOfBuilder = overrideBuilder(SIZE_OF, TypeName.get(int.class));
        MethodSpec.Builder readBuilder = overrideBuilder("read", VOID, INPUT_ARG);
        if (!all.isEmpty()) {
            sizeOfBuilder.addStatement("int size = 0");
            readBuilder.addStatement(CodeBlock.of("int number = 0"))
                .beginControlFlow("while((number = $T.$L($L)) != 0)", READER, "readNumber", INPUT)
                .beginControlFlow("switch(number)");

            for (Map.Entry<Integer, Map.Entry<String, TypeName>> entry : all.entrySet()) {
                Integer number = entry.getKey();
                String fieldName = entry.getValue().getKey();
                TypeName fieldType = entry.getValue().getValue();
                if (real.containsKey(fieldName)) {
                    writeBuilder.addStatement(makeWriteStatement(fieldName, fieldType, real.get(fieldName)));
                    sizeOfBuilder.addStatement(makeSizeOfStatement(fieldName, fieldType, real.remove(fieldName)));
                }
                readBuilder.addStatement(makeReadStatement(fieldName, fieldType, number));
            }

            readBuilder.addStatement("default: $T.$L($L)", READER, "skip", INPUT).endControlFlow().endControlFlow();
            sizeOfBuilder.addStatement("return size");
        } else {
            sizeOfBuilder.addStatement("return 0");
        }
        builder.addMethod(writeBuilder.build()).addMethod(readBuilder.build()).addMethod(sizeOfBuilder.build());
    }

    private MethodSpec.Builder methodBuilder(String name, TypeName returnType, Modifier... modifiers) {
        return MethodSpec.methodBuilder(name).returns(returnType).addModifiers(modifiers);
    }

    private MethodSpec.Builder overrideBuilder(String name, TypeName returnType, ParameterSpec... parameter) {
        MethodSpec.Builder builder = methodBuilder(name, returnType, PUBLIC)
            .addAnnotation(Override.class)
            .returns(returnType);
        for (ParameterSpec parameterSpec : parameter) {
            builder.addParameter(parameterSpec);
        }
        return builder;
    }
}