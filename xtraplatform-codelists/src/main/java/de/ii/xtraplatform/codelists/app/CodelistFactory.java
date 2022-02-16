package de.ii.xtraplatform.codelists.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.assisted.AssistedFactory;
import de.ii.xtraplatform.auth.app.ImmutableUserData;
import de.ii.xtraplatform.auth.app.User;
import de.ii.xtraplatform.auth.app.User.UserData;
import de.ii.xtraplatform.codelists.domain.Codelist;
import de.ii.xtraplatform.codelists.domain.CodelistData;
import de.ii.xtraplatform.store.domain.entities.AbstractEntityFactory;
import de.ii.xtraplatform.store.domain.entities.EntityData;
import de.ii.xtraplatform.store.domain.entities.EntityDataBuilder;
import de.ii.xtraplatform.store.domain.entities.EntityFactory;
import de.ii.xtraplatform.store.domain.entities.PersistentEntity;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@AutoBind
public class CodelistFactory extends AbstractEntityFactory<CodelistData, CodelistEntity> implements EntityFactory {

  @Inject
  public CodelistFactory(CodelistFactoryAssisted codelistFactoryAssisted) {
    super(codelistFactoryAssisted);
  }

  @Override
  public String type() {
    return Codelist.ENTITY_TYPE;
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return CodelistEntity.class;
  }

  @Override
  public EntityDataBuilder<CodelistData> dataBuilder() {
    return new ImmutableCodelistData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return CodelistData.class;
  }

  @AssistedFactory
  public interface CodelistFactoryAssisted extends FactoryAssisted<CodelistData, CodelistEntity> {
    @Override
    CodelistEntity create(CodelistData data);
  }
}
